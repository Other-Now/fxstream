package com.fxstream.app.trades;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface TradeRepository extends JpaRepository<Trade, Long> {
    Optional<Trade> findByDedupeKey(String dedupeKey);

    List<Trade> findByClientOrderId(String clientOrderId);
}
