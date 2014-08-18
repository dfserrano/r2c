package com.ssrg.r2c.usage.sql;

public class QueryFilter {

	private QueryAttribute leftAttribute;
	private QueryAttribute rightAttribute;

	public QueryFilter(QueryAttribute qa) {
		leftAttribute = qa;
	}

	public QueryFilter(QueryAttribute qa1, QueryAttribute qa2) {
		leftAttribute = qa1;
		rightAttribute = qa2;
	}

	public QueryAttribute getLeftAttribute() {
		return leftAttribute;
	}

	public QueryAttribute getRightAttribute() {
		return rightAttribute;
	}

}
