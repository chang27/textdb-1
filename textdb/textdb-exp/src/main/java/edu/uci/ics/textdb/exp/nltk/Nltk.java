package edu.uci.ics.textdb.exp.nltk;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

public class Nltk implements IOperator {

	private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
    public Nltk(){
    	
    }
    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {  
            throw new RuntimeException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    private Schema transformSchema(Schema inputSchema){
    	
        return Utils.addAttributeToSchema(inputSchema, 
                new Attribute("Test Case", AttributeType.STRING));
    }
	@Override
	public void open() throws TextDBException {
		if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();
     // generate output schema by transforming the input schema
        outputSchema = transformSchema(inputOperator.getOutputSchema());
        
        cursor = OPENED;
		
	}

	@Override
	public Tuple getNextTuple() throws TextDBException {
		if (cursor == CLOSED) {
            return null;
        }
        Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }
        
        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(inputTuple.getFields());
        outputFields.add(new StringField("abc"));
        
        return new Tuple(outputSchema, outputFields);
	}

	@Override
	public void close() throws TextDBException {
		if (cursor == CLOSED) {
            return;
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
		
	}

	@Override
	public Schema getOutputSchema() {
		return this.outputSchema;
	}
	
}
