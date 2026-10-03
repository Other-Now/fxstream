package com.fxstream.app;

import com.fxstream.app.audit.QuoteDoc;
import com.fxstream.app.fix.FixPriceIntake;
import com.fxstream.app.prices.PricingEngines;
import com.fxstream.app.quotes.QuoteService;
import com.fxstream.app.trades.Trade;
import com.fxstream.app.trades.TradeListener;
import com.fxstream.app.trades.TradeRepository;
import com.fxstream.app.trades.TradeService;
import com.fxstream.core.PairEngine;
import com.fxstream.core.Pricer;
import com.fxstream.core.Px;
import com.fxstream.core.QuoteCheck.Side;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ApiController {
    private final QuoteService quotes;
    private final PricingEngines engines;
    private final TradeRepository trades;
    private final TradeService tradeService;
    private final TradeListener listener;
    private final FixPriceIntake fix;
    private final FxProperties fx;

    public ApiController(QuoteService quotes, PricingEngines engines, TradeRepository trades, TradeService tradeService,
                         TradeListener listener, FixPriceIntake fix, FxProperties fx) {
        this.quotes = quotes;
        this.engines = engines;
        this.trades = trades;
        this.tradeService = tradeService;
        this.listener = listener;
        this.fix = fix;
        this.fx = fx;
    }

    public record QuoteRequest(@NotBlank String clientId, @NotBlank String pair, @NotNull Side side,
                               @Positive long qty, Long ttlMs) {}

    public record QuoteResponse(String quoteId, String pair, String side, long qty, long price, String priceText,
                                long expiresAtEpochMs) {}

    @PostMapping("/quotes")
    public ResponseEntity<QuoteResponse> quote(@Valid @RequestBody QuoteRequest r) {
        QuoteDoc q = quotes.issue(r.clientId(), r.pair(), r.side(), r.qty(), r.ttlMs());
        return ResponseEntity.status(HttpStatus.CREATED).body(new QuoteResponse(q.quoteId(), q.pair(), q.side(),
                q.qty(), q.price(), Px.format(q.price()), q.expiresAt().toEpochMilli()));
    }

    @GetMapping("/prices")
    public List<Map<String, Object>> prices() {
        Pricer p = engines.pricer();
        List<Map<String, Object>> out = new ArrayList<>();
        for (PairEngine e : engines.all()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("pair", e.pair().symbol());
            m.put("seq", e.seq());
            m.put("freshLps", e.freshLps());
            m.put("bestBid", Px.format(e.bestBid()));
            m.put("bestAsk", Px.format(e.bestAsk()));
            List<String> tiers = new ArrayList<>();
            for (int t = 0; t < p.tiers(); t++) {
                tiers.add(Px.format(p.clientBid(e.pair(), t, 0, e.bestBid())) + " / "
                        + Px.format(p.clientAsk(e.pair(), t, 0, e.bestAsk())));
            }
            m.put("tiers", tiers);
            out.add(m);
        }
        return out;
    }

    @GetMapping("/trades/{clientOrderId}")
    public List<Trade> trade(@PathVariable String clientOrderId) {
        return trades.findByClientOrderId(clientOrderId);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("idempotent", fx.idempotent());
        m.put("fixQuotes", fix.quotes());
        m.put("fixRejected", fix.rejected());
        long ticks = 0, prices = 0, crossed = 0, ringFull = 0;
        for (PairEngine e : engines.all()) {
            ticks += e.ticks(); prices += e.prices(); crossed += e.crossed(); ringFull += e.ringFull();
        }
        m.put("engineTicks", ticks);
        m.put("bookChanges", prices);
        m.put("crossedSkipped", crossed);
        m.put("ringFullSpins", ringFull);
        m.put("pricesPublished", engines.publisher().published());
        m.put("pricesDropped", engines.publisher().dropped());
        m.put("tradesFilled", tradeService.filled());
        m.put("tradesRejected", tradeService.rejected());
        m.put("tradesReplayed", tradeService.replayed());
        m.put("tradeInsertRacesCaught", tradeService.raceCaught());
        m.put("tradeRequestsDeadLettered", listener.poison());
        m.put("tradeRequestsRetried", listener.retried());
        return m;
    }

    @ExceptionHandler(QuoteService.NoPriceException.class)
    ResponseEntity<Map<String, String>> noPrice(QuoteService.NoPriceException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> bad(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
