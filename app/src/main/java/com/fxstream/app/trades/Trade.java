package com.fxstream.app.trades;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * One row per trade request outcome - fills and rejects alike, so that replaying a request returns the
 * same answer it got the first time.
 *
 * <p>{@code dedupeKey} carries the UNIQUE constraint that makes processing idempotent. It is the
 * client's order id; only in the chaos control run is it set to a random value, which turns the
 * constraint off without changing the schema.
 */
@Entity
@Table(name = "trades")
public class Trade {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "trades_seq")
    @SequenceGenerator(name = "trades_seq", sequenceName = "trades_id_seq", allocationSize = 50)
    private Long id;

    @Column(name = "dedupe_key", nullable = false, unique = true, updatable = false)
    private String dedupeKey;

    @Column(name = "client_order_id", nullable = false, updatable = false)
    private String clientOrderId;

    @Column(name = "client_id", nullable = false)
    private String clientId;

    @Column(name = "quote_id", nullable = false)
    private String quoteId;

    private String pair;
    private String side;
    private Long qty;
    private Long price;

    @Column(nullable = false)
    private String status;

    private String reason;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "quote_expires_at")
    private Instant quoteExpiresAt;

    protected Trade() {}

    public Trade(String dedupeKey, String clientOrderId, String clientId, String quoteId, String pair, String side,
                 Long qty, Long price, String status, String reason, Instant receivedAt, Instant quoteExpiresAt) {
        this.dedupeKey = dedupeKey;
        this.clientOrderId = clientOrderId;
        this.clientId = clientId;
        this.quoteId = quoteId;
        this.pair = pair;
        this.side = side;
        this.qty = qty;
        this.price = price;
        this.status = status;
        this.reason = reason;
        this.receivedAt = receivedAt;
        this.quoteExpiresAt = quoteExpiresAt;
    }

    public Long getId() { return id; }
    public String getDedupeKey() { return dedupeKey; }
    public String getClientOrderId() { return clientOrderId; }
    public String getClientId() { return clientId; }
    public String getQuoteId() { return quoteId; }
    public String getPair() { return pair; }
    public String getSide() { return side; }
    public Long getQty() { return qty; }
    public Long getPrice() { return price; }
    public String getStatus() { return status; }
    public String getReason() { return reason; }
    public Instant getReceivedAt() { return receivedAt; }
    public Instant getQuoteExpiresAt() { return quoteExpiresAt; }
}
