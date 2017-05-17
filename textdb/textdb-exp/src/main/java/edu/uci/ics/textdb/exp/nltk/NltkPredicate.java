package edu.uci.ics.textdb.exp.nltk;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.uci.ics.textdb.api.constants.DataConstants.TextdbProject;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.PredicateBase;

public class NltkPredicate extends PredicateBase {
    
    public static void main(String[] args) {
        String path = Utils.getResourcePath("python", TextdbProject.TEXTDB_EXP);   
        System.out.println(path);
    }
    
    @JsonCreator
    public NltkPredicate() {};

    @Override
    public Nltk newOperator() {
        
        return new Nltk();
    }

}
