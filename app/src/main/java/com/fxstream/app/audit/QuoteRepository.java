package com.fxstream.app.audit;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface QuoteRepository extends MongoRepository<QuoteDoc, String> {}
