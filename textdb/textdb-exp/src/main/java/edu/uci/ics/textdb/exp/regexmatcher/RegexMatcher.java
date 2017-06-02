package edu.uci.ics.textdb.exp.regexmatcher;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chang on 3/25/17.
 *
 * @author changliu
 * @author jun ma
 */
public class RegexMatcher extends AbstractSingleInputOperator {

    private final RegexPredicate predicate;
    private static final String labelSyntax = "<[^<>]*>";
    //(<[^<>]*>[+]*)

    // two available regex engines, RegexMatcher will try RE2J first
    private enum RegexEngine {
        JavaRegex, RE2J
    }

    private Map<Integer, String> idLabelMapping;
    private Map<Integer, String> suffixMapping;
    private String regexMod;

    private Schema inputSchema;

    public RegexMatcher(RegexPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        idLabelMapping = new HashMap<>();
        suffixMapping = new HashMap<>();
        regexMod = extractLabels(predicate.getRegex(), idLabelMapping, suffixMapping);

        if (this.inputSchema.containsField(predicate.getSpanListName())) {
            throw new DataFlowException(ErrorMessages.DUPLICATE_ATTRIBUTE(predicate.getSpanListName(), inputSchema));
        }
        outputSchema = Utils.addAttributeToSchema(inputSchema,
                new Attribute(predicate.getSpanListName(), AttributeType.LIST));

    }


    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
                inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
            }
            resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                break;
            }
        }

        return resultTuple;
    }

    /**
     * This function returns a list of spans in the given tuple that match the
     * regex For example, given tuple ("george watson", "graduate student", 23,
     * "(949)888-8888") and regex "g[^\s]*", this function will return
     * [Span(name, 0, 6, "g[^\s]*", "george watson"), Span(position, 0, 8,
     * "g[^\s]*", "graduate student")]
     *
     * @param inputTuple document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence
     * in the document
     * @throws DataFlowException
     */
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) {


        if (inputTuple == null) {
            return null;
        }
        List<Span> matchingResults = new ArrayList<>();
        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }
            boolean isValid = validTuple(fieldValue, suffixMapping);
            if (isValid) {
                matchingResults.addAll(processLabelledRegex(attributeName, inputTuple));
            }

        }
        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }

    /***
     *
     * @param fieldValue
     * @param suffixMapping
     * @return
     */

    private boolean validTuple(String fieldValue, Map<Integer, String> suffixMapping) {
        for (String suffix : suffixMapping.values()) {
            if (!fieldValue.contains(suffix)) {
                return false;
            }

        }
        return true;
    }


    private List<Span> processLabelledRegex(String attributeName, Tuple inputTuple) {
        Map<Integer, List<Span>> labelSpanList = createLabelledSpanList(inputTuple, idLabelMapping);
        String fieldValue = inputTuple.getField(attributeName).getValue().toString();
        List<Span> matchResult = labelledRegexMatcher(fieldValue, attributeName, labelSpanList, suffixMapping);

        return matchResult;
    }


    @Override
    protected void cleanUp() throws DataFlowException {
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }

    private String extractLabels(String generalRegexPattern, Map<Integer, String> idLabelMapping, Map<Integer, String> suffixMapping) {
        Pattern pattern = Pattern.compile(labelSyntax, Pattern.CASE_INSENSITIVE);
        Matcher match = pattern.matcher(generalRegexPattern);
        int key = 0;
        int pre = 0;
        int id = 1;
        String regexMod = generalRegexPattern;
        while (match.find()) {
            int start = match.start();
            int end = match.end();
            String suffix = generalRegexPattern.substring(pre, start);
            suffixMapping.put(key, suffix);
            String substr = generalRegexPattern.substring(start + 1, end - 1);
            String substrWithoutSpace = substr.replaceAll("\\s+", "");

            idLabelMapping.put(id, substrWithoutSpace);

            regexMod = regexMod.replace("<" + substr + ">", "<" + id + ">");
            id++;
            key++;
            pre = end;
        }
        suffixMapping.put(key, generalRegexPattern.substring(pre));
        return regexMod;
    }

    private List<Span> labelledRegexMatcher(String fieldValue, String attributeName, Map<Integer, List<Span>> labelIDSpanMap, Map<Integer, String> suffixMap) throws DataFlowException {
        List<Span> matchingResults = new ArrayList<>();

        Map<Integer, List<Span>> temp = new HashMap<>();
        for (Integer key : labelIDSpanMap.keySet()) {
            temp.put(key, labelIDSpanMap.get(key));
        }
        List<int[]> indexs = generateAllCombinationsOfRegex(attributeName, fieldValue, suffixMap, temp);
        if (!indexs.isEmpty()) {
            for (int[] entry : indexs) {
                String spanValue = fieldValue.substring(entry[0], entry[1]);
                Span e = new Span(attributeName, entry[0], entry[1], predicate.getRegex(), spanValue);
                matchingResults.add(e);
            }
        }
        return matchingResults;
    }

    /***
     *
     * @param suffixMap: regex -- token position except from label
     * @param labelSpanList: label id --- spanlist
     * @param attrName: regex tokens start position
     * @return
     */
    private List<int[]> generateAllCombinationsOfRegex(String attrName, String fieldValue, Map<Integer, String> suffixMap, Map<Integer, List<Span>> labelSpanList) {

        if (suffixMap.get(0).length() != 0) {
            String toMatch = suffixMap.get(0);
            for (Span span : labelSpanList.get(1)) {
                if (!span.getAttributeName().equals(attrName)) {
                    labelSpanList.get(1).remove(span);
                  continue;
                }
                int start = span.getStart() - toMatch.length();
                if (!fieldValue.substring(start, start + toMatch.length()).equals(toMatch)) {
                    labelSpanList.get(1).remove(span);
                } else {
                    Span newSpan = new Span(span.getAttributeName(), start, span.getEnd(), span.getKey(), toMatch +span.getValue() );
                    labelSpanList.get(1).remove(span);
                    labelSpanList.get(1).add(newSpan);

                }
            }
        }

        List<int[]> resultArray = new ArrayList<>();
        for (int i = 1; i <= labelSpanList.size(); i++) {
            String suffix = suffixMap.get(i);
            for (Span span : labelSpanList.get(i)) {
                if (!span.getAttributeName().equals(attrName)) {
                    labelSpanList.get(i).remove(span);
                    continue;
                }
                if (suffix.length() != 0) {
                    if (!fieldValue.substring(span.getEnd(), span.getEnd() + suffix.length()).equals(suffix)) {
                        labelSpanList.get(i).remove(span);
                    } else {
                        int end = span.getEnd()+ suffix.length();
                        Span newSpan = new Span(span.getAttributeName(), span.getStart(), end, span.getKey(), span.getValue()+suffix);
                        labelSpanList.get(i).remove(span);
                        labelSpanList.get(i).add(newSpan);
                       // span.setEnd(span.getEnd() + suffix.length());
                    }
                }

            }
            if (labelSpanList.get(i).isEmpty()) return resultArray;
        }
        for (int i = 1; i <= labelSpanList.size(); i++) {

            List<Span> sort = SortSpanlist(labelSpanList.get(i));
            labelSpanList.put(i, sort);
        }


        resultArray = generateCombination(labelSpanList);

        return resultArray;
    }

    private List<int[]> generateCombination(Map<Integer, List<Span>> labelSpanList) {
        List<int[]> res = new ArrayList<>();
        for (Span span : labelSpanList.get(1)) {
            boolean isValid = true;
            int[] iArray = new int[2];
            if (labelSpanList.size() == 1) {
                iArray[0] = span.getStart();
                iArray[1] = span.getEnd();
                //       iArray[1] = span.getEnd();
            } else {
                iArray[0] = span.getStart();
                int index = 2;
                int start = span.getEnd();
                while (index <= labelSpanList.size()) {
                    Span next = binarySearch(labelSpanList.get(index), start);
                    if (next.getStart() != -1 && next.getValue()!=null) {
                        if (index == labelSpanList.size()) {
                            iArray[1] = next.getEnd();
                        } else if (index < labelSpanList.size()) {
                            start = next.getEnd();
                        }
                        index++;
                    } else {
                        isValid = false;
                        break;

                    }
                }

            }
            if (isValid && iArray[0] >= 0 && iArray[1] > 0) {
                res.add(iArray);
            }
        }
        return res;

    }


    private Map<Integer, List<Span>> createLabelledSpanList(Tuple inputTuple, Map<Integer, String> idLabelMapping) {
        Map<Integer, List<Span>> labelSpanList = new HashMap<>();
        for (int id : idLabelMapping.keySet()) {
            String labels = idLabelMapping.get(id);
            List<Span> values = new ArrayList<>();

            ListField<Span> spanListField = inputTuple.getField(labels);
            List<Span> spanList = spanListField.getValue();


            values.addAll(spanList);

            labelSpanList.put(id, values);
        }
        return labelSpanList;
    }

    private List<Span> SortSpanlist(List<Span> spanList) {
        Collections.sort(spanList, new Comparator<Span>() {
            @Override
            public int compare(Span o1, Span o2) {
                return o1.getStart() - o2.getStart();
            }
        });

        return spanList;
    }

    private static Span binarySearch(List<Span> list, int index) {
        int start = 0;
        int end = list.size() - 1;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            if (list.get(mid).getStart() == index) {
                return list.get(mid);
            } else if (list.get(mid).getStart() < index) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }
        Span nul = new Span("nul", -1, -1, "offset", "null");
        return nul;
    }
}
