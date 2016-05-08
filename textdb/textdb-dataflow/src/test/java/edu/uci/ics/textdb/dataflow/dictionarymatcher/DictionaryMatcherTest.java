
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author rajeshyarlagadda
 *
 */
public class DictionaryMatcherTest {

    private DictionaryMatcher dictionaryMatcher;
    private DataStore dataStore;
    private IDataWriter dataWriter;
    private IDataReader dataReader;
    private Analyzer analyzer;
    private Query luceneQuery;
    private IPredicate dataReaderPredicate;

    @Before
    public void setUp() throws Exception {

        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        analyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, analyzer);
        QueryParser luceneQueryParser = new QueryParser(TestConstants.ATTRIBUTES_PEOPLE[0].getFieldName(), analyzer);
        luceneQuery = luceneQueryParser.parse(DataConstants.SCAN_QUERY);
        dataReaderPredicate = new DataReaderPredicate(dataStore, luceneQuery);
        dataReader = new DataReader(dataReaderPredicate);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());

    }

    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }

    public List<ITuple> getQueryResults(IDictionary dictionary, ISourceOperator sourceOperator,
            List<Attribute> attributes) throws Exception {

        dictionaryMatcher = new DictionaryMatcher(dictionary, sourceOperator, attributes);
        dictionaryMatcher.open();
        ITuple nextTuple = null;
        List<ITuple> results = new ArrayList<ITuple>();
        while ((nextTuple = dictionaryMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        dictionaryMatcher.close();
        return results;
    }

    /**
     * Scenario S1:verifies GetNextTuple of Dictionary
     * 
     * checking if Dictionary returns all the strings given by the user
     */

    @Test
    public void testGetNextOfDictionaryItem() throws Exception {

        ArrayList<String> expectedStrings = new ArrayList<String>(Arrays.asList("george", "lee", "bruce"));
        IDictionary dictionary = new Dictionary(expectedStrings);
        int numTuples = 0;
        String dictionaryItem;
        ArrayList<String> returnedStrings = new ArrayList<String>();
        while ((dictionaryItem = dictionary.getNextValue()) != null) {
            returnedStrings.add(dictionaryItem);
            numTuples++;
        }
        boolean contains = TestUtils.containsAllResults(expectedStrings, returnedStrings);
        Assert.assertTrue(contains);
        Assert.assertEquals(3, numTuples);

    }

    /**
     * Scenario S-2(a):verifies GetNextTuple of DictionaryMatcher and single
     * word queries in String Field
     */

    @Test
    public void testSingleWordQueryInStringField() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("Bruce"));
        IDictionary dictionary = new Dictionary(names);
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 5, "Bruce", "bruce");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, sourceOperator, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S- 2(b):verifies GetNextTuple of DictionaryMatcher and single
     * word queries in Text Field
     */

    @Test
    public void testSingleWordQueryInTextField() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall"));
        IDictionary dictionary = new Dictionary(names);
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        expectedResults.add(tuple2);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, sourceOperator, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S3:verifies ITuple returned by DictionaryMatcher and multiple
     * word queries
     */

    @Test
    public void testMultipleWordsQuery() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("george lin lin"));
        IDictionary dictionary = new Dictionary(names);
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, sourceOperator, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario S4:verifies: data source has multiple attributes, and an entity
     * can appear in all the fields and multiple times.
     */

    @Test
    public void testWordInMultipleFieldsQuery() throws Exception {

        ArrayList<String> names = new ArrayList<String>(Arrays.asList("lin clooney"));
        IDictionary dictionary = new Dictionary(names);
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
        // create data tuple first
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 11, "lin clooney", "lin clooney");
        Span span2 = new Span("description", 0, 11, "lin clooney", "Lin Clooney");
        Span span3 = new Span("description", 25, 36, "lin clooney", "lin clooney");
        // Span span3 = new Span("lastName", 0, 3, "Lin", "lin");
        list.add(span1);
        list.add(span2);
        list.add(span3);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResults = new ArrayList<ITuple>();
        expectedResults.add(tuple1);
        List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
                TestConstants.DESCRIPTION_ATTR);

        List<ITuple> returnedResults = getQueryResults(dictionary, sourceOperator, attributes);
        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }
}
