package com.cs.rfq.decorator;

import org.junit.Test;
import static org.junit.Assert.*;

public class RfqMetadataTest {
    @Test
    public void checkLiquidityMetadata(){
        RfqMetadata rfqMetadata = new RfqMetadata();
        String liquidity = rfqMetadata.lookupLiquidity("AT0000A0U3T4");
        String liquidityKeyErorr = rfqMetadata.lookupLiquidity("A most likely non-existing instrument");
        assertEquals("114.44", liquidity);
        assertEquals("-", liquidityKeyErorr);
    }
}
