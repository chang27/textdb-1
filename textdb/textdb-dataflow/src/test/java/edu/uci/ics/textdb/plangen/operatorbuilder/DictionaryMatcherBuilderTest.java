package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcher;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import junit.framework.Assert;

public class DictionaryMatcherBuilderTest {
    
    /*
     * test DictionaryMatcherBuilder with the following properties:
     *   keyword: Irvine
     *   attribute list: {city, STRING}, {location, STRING}, {content, TEXT}
     *   matching type: PHRASE_INDEXBASED
     *   limit: 10
     *   offset: 2
     * 
     */
    @Test
    public void testDictionaryMatcherBuilder() throws PlanGenException, DataFlowException {
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
        List<String> dictionaryList = Arrays.asList(
                "Irvine",
                "Anaheim",
                "Costa Mesa",
                "Santa Ana");
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "10");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        DictionaryMatcher dictionaryMatcher = DictionaryMatcherBuilder.buildOperator(operatorProperties);
               
        String dictEntry = null;
        ArrayList<String> actualDict = new ArrayList<>();
        while ((dictEntry = dictionaryMatcher.getPredicate().getNextDictionaryEntry()) != null) {
            actualDict.add(dictEntry);
        }
        Assert.assertEquals(dictionaryList.toString(), actualDict.toString());
        List<Attribute> irvineAttrList = Arrays.asList(
                new Attribute("city", FieldType.STRING),
                new Attribute("location", FieldType.STRING),
                new Attribute("content", FieldType.TEXT));
        Assert.assertEquals(irvineAttrList.toString(), dictionaryMatcher.getPredicate().getAttributeList().toString());
        Assert.assertEquals(KeywordMatchingType.PHRASE_INDEXBASED, dictionaryMatcher.getPredicate().getKeywordMatchingType());
        Assert.assertEquals(10, dictionaryMatcher.getLimit());
        Assert.assertEquals(2, dictionaryMatcher.getOffset()); 
    }
    
    /*
     * test invalid DictionaryMatcherBuilder by a missing dictionary property
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidDictionaryMatcherBuilder() throws PlanGenException, DataFlowException {
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "10");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        DictionaryMatcherBuilder.buildOperator(operatorProperties);
    }

}
