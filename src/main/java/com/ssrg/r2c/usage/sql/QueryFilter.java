package com.ssrg.r2c.usage.sql;

public class QueryFilter {

	private QueryAttribute leftAttribute;
	private String operator;
	private QueryAttribute rightAttribute;

	public QueryFilter(QueryAttribute qa) {
		leftAttribute = qa;
		this.operator = "=";
	}

	public QueryFilter(QueryAttribute qa1, QueryAttribute qa2, String operator) {
		leftAttribute = qa1;
		rightAttribute = qa2;
		this.operator = operator;
	}

	public QueryAttribute getLeftAttribute() {
		return leftAttribute;
	}

	public QueryAttribute getRightAttribute() {
		return rightAttribute;
	}
	
	public String getOperator() {
		return operator;
	}

}
