package com.cs.rfq.decorator.extractors;

/**
 * Enumeration of all metadata that will be published by this component
 */
public enum RfqMetadataFieldNames {
    id,
    traderId,
    entityId,
    instrumentId,
    qty,
    price,
    side,
    tradesWithEntityPastWeek,
    tradesWithEntityPastMonth,
    tradesWithEntityPastYear,
    totalVolumeTradedForInstrumentPastWeek,
    totalVolumeTradedForInstrumentPastMonth,
    totalVolumeTradedForInstrumentPastYear,
    instrumentLiquidity,
    tradeBiasMonthToDate,
    tradeBiasWeekToDate,
    averageTradedPrice
}
