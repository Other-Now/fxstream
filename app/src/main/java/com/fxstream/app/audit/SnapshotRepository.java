package com.fxstream.app.audit;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface SnapshotRepository extends MongoRepository<PriceSnapshotDoc, String> {
    List<PriceSnapshotDoc> findTop10ByPairOrderByTsDesc(String pair);
}
