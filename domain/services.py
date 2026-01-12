def latency_ms(t_arrival_epoch: float, t_meas_epoch: float) -> float:
    return (t_arrival_epoch - t_meas_epoch) * 1000.0