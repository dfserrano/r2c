package com.ssrg.r2c.usage.sql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Query {

	private List<QueryAttribute> projections;
	private List<String> relations;
	private Set<QueryFilter> selections;
	private long timesUsed = 0;

	public Query() {
		this.projections = new ArrayList<QueryAttribute>();
		this.relations = new ArrayList<String>();
		this.selections = new HashSet<QueryFilter>();
		this.timesUsed = 0;
	}

	public Query(List<QueryAttribute> projections, List<String> relations,
			Set<QueryFilter> selections) {
		this.projections = projections;
		this.relations = relations;
		this.selections = selections;
		this.timesUsed = 0;
	}
	
	public Query(List<QueryAttribute> projections, List<String> relations,
			Set<QueryFilter> selections, long initialCount) {
		this(projections, relations, selections);
		this.timesUsed = initialCount;
	}

	public void addTimedUsed(int count) {
		this.timesUsed += count;
	}

	public List<QueryAttribute> getProjections() {
		return projections;
	}
	
	public void addProjection(QueryAttribute qa) {
		if (!projections.contains(qa))
			projections.add(qa);
	}

	public List<String> getRelations() {
		return relations;
	}

	public void addRelation(String relation) {
		if (!relations.contains(relation))
			relations.add(relation);
	}
	
	public Set<QueryFilter> getSelections() {
		return selections;
	}
	
	public void addSelection(QueryFilter filter) {
		if (!selections.contains(filter))
			selections.add(filter);
	}

	public long getTimesUsed() {
		return timesUsed;
	}

	public void mergeWith(Query query) {
		projections.addAll(query.getProjections());
		relations.addAll(query.getRelations());
		selections.addAll(query.getSelections());
		timesUsed += query.getTimesUsed();
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		sb.append("SELECT ");
		for (QueryAttribute projection : projections) {
			sb.append(projection.getTableName() + "."
					+ projection.getColumnName() + ", ");
		}
		if (!projections.isEmpty())
			sb.delete(sb.length() - 2, sb.length());
		else
			sb.append("*");

		sb.append(" FROM ");
		for (String relation : relations) {
			sb.append(relation + ", ");
		}
		if (!relations.isEmpty())
			sb.delete(sb.length() - 2, sb.length());

		sb.append(" WHERE ");
		for (QueryFilter selection : selections) {
			sb.append(selection.getLeftAttribute().getTableName() + "."
					+ selection.getLeftAttribute().getColumnName() + " = "
					+ selection.getRightAttribute().getTableName() + "."
					+ selection.getRightAttribute().getColumnName() + " AND ");
		}
		if (!selections.isEmpty())
			sb.delete(sb.length() - 5, sb.length());
		else 
			sb.delete(sb.length() - 7, sb.length());
		
		return sb.toString();
	}

}
