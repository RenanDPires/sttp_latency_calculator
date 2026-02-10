from __future__ import annotations

# Tipos usados para deixar explícita a estrutura de dados interna:
# - Dict / Tuple / Set para caches e assinaturas
# - Optional para estados que começam "não inicializados"
from typing import Dict, Tuple, List, Optional, Set

# Subscriber STTP (callback-based) e itens de configuração/conexão
from sttp.subscriber import Subscriber
from sttp.config import Config
from sttp.settings import Settings

# Measurement = unidade recebida do STTP (carrega valor, timestamp, flags, etc.)
from sttp.transport.measurement import Measurement

# Cache do mapeamento SignalID -> metadados/signal index (vem no subscription update)
from sttp.transport.signalindexcache import SignalIndexCache

# Núcleo da aplicação (domínio / pipeline):
# - Clock: abstração do relógio (facilita testes e padroniza epoch)
# - KeyExtractor: extrai uma chave lógica (PPA) do Measurement+metadata
# - LatencyAuditEvent: registro detalhado para auditoria (por janela)
# - LatencyEvent: evento "leve" para stats/latência no pipeline
# - LatencyPipeline: processador/acumulador de eventos (janela, flush, etc.)
# - ThresholdMonitor: regras adicionais de violação (sobre value, por PPA)
# - ViolationEvent / ViolationSink: emissão/persistência de violações
# - WindowAuditSink: persistência de auditoria por janela
from core import (
    Clock,
    KeyExtractor,
    LatencyAuditEvent,
    LatencyEvent,
    LatencyPipeline,
    ThresholdMonitor,
    ViolationEvent,
    ViolationSink,
    WindowAuditSink,
)

# Infra para registrar measurements brutos em CSV (assíncrono):
# Útil para forense, debug e reprocessamento.
from infra.raw_measurement_csv_sink import AsyncCsvRawMeasurementWriter, build_raw_measurement_record


class SttpSubscriber(Subscriber):
    # Subscriber especializado em:
    # 1) receber measurements via STTP
    # 2) calcular latência (arrival - measurement timestamp)
    # 3) auditar (por janela) e emitir violações (latência < 0, thresholds)
    # 4) alimentar um pipeline de stats (somente para chaves/PPAs habilitados)
    # 5) deduplicar frames (batch + TTL) para evitar contagens/estatísticas infladas
    def __init__(
        self,
        pipeline: LatencyPipeline,
        clock: Clock,
        key_extractor: KeyExtractor,
        *,
        # stats_keys: conjunto de PPAs habilitados para entrar no pipeline de stats (latência/frames)
        stats_keys: Optional[Set[int]] = None,
        # monitor de regras adicionais (sobre o "value" da medida) independente do pipeline
        threshold_monitor: Optional[ThresholdMonitor] = None,
        # sink onde eventos de violação são publicados
        violation_sink: Optional[ViolationSink] = None,
        # sink para auditoria por janela (persistência em lote)
        window_audit_sink: Optional[WindowAuditSink] = None,
        # se True, grava auditoria apenas quando houver latência negativa na janela (modo "on-negative")
        audit_on_negative_latency: bool = True,
        # se True, loga um "raw" detalhado em caso de latência negativa (para debug em tempo real)
        log_raw_on_negative: bool = False,
        # sink opcional para salvar *todas* as medições brutas (inclusive duplicadas, pois vem antes do dedupe)
        raw_measurement_sink: Optional[AsyncCsvRawMeasurementWriter] = None,
        # -----------------------------
        # Alinhamento
        # -----------------------------
        # align_window_sec: tamanho do alinhamento e também usado como janela de auditoria (por padrão 10s)
        # “alinhamento” aqui significa: descartar até chegar o primeiro boundary (X0) do relógio
        align_window_sec: int = 10,
    ):
        super().__init__()

        # Config/Settings do STTP. Em geral, o Subscriber usa isso para conexão/parametrização
        self.config = Config()
        self.settings = Settings()

        # Dependências principais do fluxo:
        # - pipeline: onde eventos (LatencyEvent) serão acumulados e processados
        # - clock: fonte do tempo de chegada (arrival_epoch)
        # - key_extractor: resolve PPA/chave lógica a partir da medida
        self.pipeline = pipeline
        self.clock = clock
        self.key_extractor = key_extractor

        # PPAs que entram no pipeline de stats (latência/frames).
        # Converte tudo para int e protege contra None.
        self.stats_keys: Set[int] = set(int(x) for x in (stats_keys or set()))

        # Monitor de violações (medidas) - independente do pipeline.
        # Observação: só publica se violation_sink também estiver configurado.
        self.threshold_monitor = threshold_monitor
        self.violation_sink = violation_sink

        # Auditoria de latência (persistência por janela).
        # Mantém um buffer (_audit_events) que será escrito em lote no window_audit_sink.
        self.window_audit_sink = window_audit_sink
        self.audit_on_negative_latency = audit_on_negative_latency
        self.log_raw_on_negative = log_raw_on_negative
        self.raw_measurement_sink = raw_measurement_sink

        # Janela de auditoria (sec). Usa o mesmo parâmetro de alinhamento.
        self._audit_window_sec = int(align_window_sec)

        # Estado da janela atual: start (epoch) e lista de eventos coletados
        self._audit_window_start: Optional[float] = None
        self._audit_events: List[LatencyAuditEvent] = []
        # Flag da janela atual: se ocorreu qualquer latência negativa, pode habilitar escrita condicional
        self._audit_has_negative = False

        # Flag para log inicial de “começou a receber”
        self._started = False

        # -----------------------------
        # Align gate (descarta até o próximo X0)
        # -----------------------------
        # Objetivo: garantir que o processamento comece alinhado num múltiplo de N segundos.
        # Isso é útil para janelas determinísticas (10s alinhadas) e para comparar relatórios.
        self._aligned: bool = False
        self._align_window_sec: int = int(align_window_sec)
        # Epoch inteiro do boundary-alvo (próximo múltiplo de align_window_sec)
        self._align_target_epoch: Optional[float] = None
        # Contador de frames descartados durante o gating (antes do alinhamento)
        self._dropped_before_align: int = 0

        # -----------------------------
        # Dedupe (drop de frames repetidos)
        # -----------------------------
        # TTL = Time To Live: por quanto tempo uma assinatura (PPA, t_meas_epoch) é considerada “recente”.
        # Se a mesma assinatura reaparecer dentro desse TTL, é descartada.
        self._dedupe_ttl_s: float = 5.0  # janela de dedupe entre batches -- Time To Live

        # Cache de dedupe:
        # chave = (ppa, t_meas_epoch), valor = last_seen_arrival_epoch (quando vimos por último)
        self._dedupe_seen: Dict[Tuple[int, float], float] = {}

        # A cada N frames processados, faz uma limpeza do cache para não crescer indefinidamente
        self._dedupe_cleanup_every: int = 2000
        self._dedupe_i: int = 0

        # Registra os callbacks no Subscriber STTP:
        # - subscription_updated: quando chega o SignalIndexCache (mapeamentos)
        # - new_measurements: callback principal com lote/batch de measurements
        # - connection_terminated: reset de estado ao desconectar
        self.set_subscriptionupdated_receiver(self.subscription_updated)
        self.set_newmeasurements_receiver(self.new_measurements)
        self.set_connectionterminated_receiver(self.connection_terminated)

    def subscription_updated(self, signalindexcache: SignalIndexCache):
        # Confirmação/observabilidade: quantos mapeamentos chegaram no SignalIndexCache.
        # Isso ajuda a validar se a subscription carregou os metadados esperados.
        self.statusmessage(f"Received signal index cache with {signalindexcache.count:,} mappings")

    # -----------------------------
    # Align helpers
    # -----------------------------
    def _compute_next_boundary_epoch(self, now_epoch: float) -> int:
        """
        Retorna o próximo múltiplo de self._align_window_sec (ex.: 10s),
        sempre como epoch inteiro (sem fração).

        Exemplo:
          - window=10
          - now_epoch=123.4  -> retorna 130
          - now_epoch=130.0  -> retorna 140 (próximo boundary)
        """
        w = int(self._align_window_sec)
        # (int(now)//w + 1)*w garante "próximo" boundary, não o atual
        return (int(now_epoch) // w + 1) * w

    def new_measurements(self, measurements: List[Measurement]):
        # Primeiro batch recebido: apenas loga uma vez
        if not self._started:
            self._started = True
            self.statusmessage("Receiving measurements...")

        # ============================
        # ALIGN GATE: descarta até o primeiro início X0
        # ============================
        # Enquanto não estiver alinhado:
        # - calcula o próximo boundary (múltiplo de N segundos)
        # - descarta batches inteiros até alcançar esse epoch
        # Observação: usa clock.now_epoch() (tempo de chegada "agora"), não o timestamp da medida.
        if not self._aligned:
            now_epoch = float(self.clock.now_epoch())

            # Define o alvo de alinhamento na primeira vez
            if self._align_target_epoch is None:
                self._align_target_epoch = float(self._compute_next_boundary_epoch(now_epoch))
                self.statusmessage(
                    f"[align] gating enabled. dropping until epoch={self._align_target_epoch:.0f} "
                    f"(window={self._align_window_sec}s)"
                )

            # Se ainda não chegou no boundary, descarta TUDO deste batch e retorna
            if now_epoch < float(self._align_target_epoch):
                self._dropped_before_align += len(measurements)

                # Log “suave”: evita spam (aprox. a cada 5000 descartados)
                if (self._dropped_before_align % 5000) < len(measurements):
                    self.statusmessage(
                        f"[align] dropped_before_align={self._dropped_before_align} "
                        f"now={now_epoch:.3f} target={self._align_target_epoch:.0f}"
                    )
                return

            # Chegou no boundary: libera o processamento normal a partir daqui
            self._aligned = True
            self.statusmessage(
                f"[align] released at now_epoch={now_epoch:.3f} target={self._align_target_epoch:.0f} "
                f"dropped={self._dropped_before_align}"
            )

        # Tempo de chegada do batch (epoch seconds).
        # Importante: usado como referência para latência e também para “janelas” de auditoria.
        arrival_epoch = self.clock.now_epoch()

        # Dedupe local deste batch:
        # evita repetir processamento se o mesmo frame veio duplicado dentro do próprio lote.
        seen_batch: Set[Tuple[int, float]] = set()

        # Contadores do batch atual (observabilidade)
        dropped_dupes = 0
        processed = 0

        for m in measurements:
            # Extrai metadata e calcula a chave lógica (PPA)
            md = self.measurement_metadata(m)
            key = int(self.key_extractor.key_from(m, md))

            # --------------------------------------------
            # (Opcional) persistência do measurement bruto
            # --------------------------------------------
            # Publica o raw ANTES do dedupe:
            # - prós: auditoria forense completa do que chegou da rede
            # - contras: duplicatas também serão registradas aqui
            if self.raw_measurement_sink is not None:
                self.raw_measurement_sink.publish(
                    build_raw_measurement_record(
                        arrival_epoch=float(arrival_epoch),
                        measurement=m,
                        metadata=md,
                        ppa_key=key,
                    )
                )

            # Timestamp da medida (origem) e cálculo de latência:
            # latency(ms) = (chegada - timestamp_da_medida) * 1000
            t_meas_epoch = float(m.datetime.timestamp())
            latency = (arrival_epoch - t_meas_epoch) * 1000.0

            # Janela de auditoria alinhada em _audit_window_sec (ex.: 10s).
            # Nota: o alinhamento é feito pelo arrival_epoch (tempo de chegada), não pelo timestamp da medida.
            window_start = (int(arrival_epoch) // self._audit_window_sec) * self._audit_window_sec

            # Inicializa janela se for a primeira amostra
            if self._audit_window_start is None:
                self._audit_window_start = float(window_start)
            # Se mudou de janela, flush da janela anterior e inicia a nova
            elif window_start != int(self._audit_window_start):
                self._flush_audit_window()
                self._audit_window_start = float(window_start)

            # ============================
            # DEDUPE GATE (batch + TTL)
            # ============================
            # Assinatura do frame: (PPA, timestamp da medida).
            # Ideia: se mesma chave e mesmo t_meas_epoch, é o “mesmo frame”.
            sig = (key, t_meas_epoch)

            # 1) Dedupe no batch (duplicata dentro do lote atual)
            if sig in seen_batch:
                dropped_dupes += 1
                continue
            seen_batch.add(sig)

            # 2) Dedupe entre batches (TTL):
            # Se já vimos essa assinatura recentemente (<= TTL), descartamos para não processar duas vezes.
            last_seen = self._dedupe_seen.get(sig)
            if last_seen is not None and (arrival_epoch - last_seen) <= self._dedupe_ttl_s:
                dropped_dupes += 1
                continue

            # Atualiza “última vez visto” para esta assinatura
            self._dedupe_seen[sig] = arrival_epoch

            # Limpeza periódica do cache de dedupe:
            # Remove assinaturas não vistas há mais que o TTL (evita crescimento infinito do dict)
            self._dedupe_i += 1
            if self._dedupe_i % self._dedupe_cleanup_every == 0:
                cutoff = arrival_epoch - self._dedupe_ttl_s
                # mantém apenas entradas cujo last_seen >= cutoff
                self._dedupe_seen = {k: v for k, v in self._dedupe_seen.items() if v >= cutoff}

            # ============================
            # Daqui pra baixo: SÓ entra se NÃO for repetido
            # ============================
            processed += 1

            # Value numérico do measurement (pode ser usado em threshold_monitor e pipeline)
            value = float(m.value)

            # --------------------------------------------
            # Auditoria detalhada por janela
            # --------------------------------------------
            # Registra tudo (arrival, meas, ppa, value, flags, latency) para possível persistência em lote.
            self._audit_events.append(
                LatencyAuditEvent(
                    t_arrival_epoch=arrival_epoch,
                    t_meas_epoch=t_meas_epoch,
                    ppa=key,
                    value=value,
                    latency_ms=latency,
                    flags=int(m.flags),
                )
            )

            # Tratamento de latência negativa:
            # - Marca a janela como “tem negativo” (pode disparar escrita condicional)
            # - Opcionalmente loga detalhes do raw para debugging rápido
            if latency < 0:
                self._audit_has_negative = True
                if self.log_raw_on_negative:
                    self.statusmessage(
                        "[raw-measurement] "
                        f"ppa={key} arrival_epoch={arrival_epoch:.6f} "
                        f"meas_epoch={t_meas_epoch:.6f} delta_ms={latency:.3f} "
                        f"value={value} flags={int(m.flags)} "
                        f"signalid={getattr(m, 'signalid', None)}"
                    )

            # Violação específica: LATENCY < 0 ms
            # Publica um evento de violação separado (para alertas, persistência, etc.)
            if latency < 0 and self.violation_sink is not None:
                self.violation_sink.publish(
                    ViolationEvent(
                        t_epoch=arrival_epoch,
                        ppa=int(key),
                        # Aqui value carrega a latência (ms) negativa, não a grandeza original
                        value=float(latency),
                        rule_id="LATENCY_LT_0",
                        rule="< 0 ms",
                    )
                )

            # (1) Violações do ThresholdMonitor:
            # Importante: só chegam aqui medidas NÃO duplicadas (o dedupe filtra antes).
            # Requer tanto threshold_monitor quanto violation_sink.
            if self.threshold_monitor is not None and self.violation_sink is not None:
                violations = self.threshold_monitor.check(
                    now_epoch=arrival_epoch,
                    ppa=key,
                    value=value,
                )
                for v in violations:
                    self.violation_sink.publish(v)

            # (2) Stats (latência / frames recebidos):
            # Apenas para PPAs explicitamente habilitados em stats_keys.
            if key not in self.stats_keys:
                continue

            # Evento “leve” para pipeline:
            # Carrega timestamps de medida e chegada, além de flags e value.
            ev = LatencyEvent(
                key=key,
                t_meas_epoch=t_meas_epoch,
                t_arrival_epoch=arrival_epoch,
                flags=int(m.flags),
                value=value,
            )
            self.pipeline.submit(ev)

        # Reporta ao pipeline o tamanho efetivamente processado (após dedupe).
        # Isso evita que estatísticas internas considerem como “entrada” frames descartados.
        self.pipeline.on_batch_received(batch_size=processed)

        # Log de warning quando houver descarte de duplicatas
        if dropped_dupes:
            self.statusmessage(
                f"[warn] dropped duplicated frames: {dropped_dupes} / {len(measurements)} (processed={processed})"
            )

        # Flush do pipeline conforme política interna (ex.: por janela/tempo/volume)
        self.pipeline.maybe_flush()

        # Flush da auditoria baseado em tempo atual (independente de troca de janela por arrival)
        self._maybe_flush_audit_by_time(self.clock.now_epoch())

    def connection_terminated(self):
        # Chama o receiver padrão do Subscriber (comportamento base da lib)
        self.default_connectionterminated_receiver()
        self._started = False

        # Reset do alinhamento ao reconectar:
        # A reconexão pode mudar o “ponto de início”, então reativamos o align gate.
        self._aligned = False
        self._align_target_epoch = None
        self._dropped_before_align = 0

        # Garante que qualquer janela de auditoria pendente seja fechada/limpa
        self._flush_audit_window()
        self._audit_window_start = None
        self._audit_events = []
        self._audit_has_negative = False

    def flush_audit_window(self) -> None:
        # Método público para forçar o flush e resetar estado.
        # Útil para shutdown controlado, testes, ou para “fechar” antes de trocar sinks.
        self._flush_audit_window()
        self._audit_window_start = None
        self._audit_events = []
        self._audit_has_negative = False

    def _maybe_flush_audit_by_time(self, now_epoch: float) -> None:
        # Flush “por tempo”: se já passamos do fim da janela atual, flush.
        # Isso cobre casos em que não houve troca de janela dentro do loop (poucos dados),
        # ou em que o processamento ficou parado e depois voltou.
        if self._audit_window_start is None:
            return

        # Fim da janela atual
        window_end = float(self._audit_window_start) + float(self._audit_window_sec)

        # Se o tempo atual já ultrapassou o fim, flush e reposiciona o início da janela
        if now_epoch >= window_end:
            self._flush_audit_window()

            # Reposiciona start para o boundary correto correspondente ao now_epoch
            self._audit_window_start = float(
                (int(now_epoch) // self._audit_window_sec) * self._audit_window_sec
            )

    def _flush_audit_window(self) -> None:
        # Flush efetivo: escreve no sink (se configurado) e limpa buffers.
        # Observação importante: se não houver sink, ainda assim limpa a memória local.
        if self.window_audit_sink is None or self._audit_window_start is None:
            self._audit_events = []
            self._audit_has_negative = False
            return

        # Decide se deve escrever:
        # - Se audit_on_negative_latency=False: escreve sempre
        # - Se audit_on_negative_latency=True: escreve só se houve latência negativa na janela
        should_write = (not self.audit_on_negative_latency) or self._audit_has_negative

        # Só escreve se houver eventos acumulados
        if should_write and self._audit_events:
            window_start = float(self._audit_window_start)
            window_end = window_start + float(self._audit_window_sec)

            # Persiste a janela: start/end + lista de eventos (copiada para evitar efeitos colaterais)
            self.window_audit_sink.write_window(window_start, window_end, list(self._audit_events))

        # Limpa buffer e flag para a próxima janela
        self._audit_events = []
        self._audit_has_negative = False
