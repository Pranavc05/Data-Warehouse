"""
Core AI Agent system for intelligent query optimization and database management
"""

import asyncio
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import openai
from langchain.llms import OpenAI
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.agents import initialize_agent, Tool, AgentType

from src.database.connection import get_db, get_async_db
from src.database.models import QueryPerformanceLog, AIOptimizationSuggestion
from config import OPENAI_API_KEY, AI_MODEL, AI_TEMPERATURE

import logging
logger = logging.getLogger(__name__)

class OptimizationType(Enum):
    INDEX = "INDEX"
    QUERY_REWRITE = "QUERY_REWRITE"
    PARTITION = "PARTITION"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"

@dataclass
class QueryAnalysis:
    query_hash: str
    query_text: str
    avg_execution_time: float
    execution_count: int
    cost_estimate: float
    bottlenecks: List[str]
    optimization_opportunities: List[str]

@dataclass
class OptimizationSuggestion:
    type: OptimizationType
    target_table: str
    description: str
    implementation_sql: str
    estimated_improvement: float
    confidence_score: float
    reasoning: str

class QueryOptimizationAgent:
    """
    Advanced AI agent for SQL query optimization and performance analysis
    """
    
    def __init__(self):
        self.llm = OpenAI(
            model_name=AI_MODEL,
            temperature=AI_TEMPERATURE,
            openai_api_key=OPENAI_API_KEY
        )
        self.setup_prompts()
        self.setup_tools()
    
    def setup_prompts(self):
        """Setup specialized prompts for different optimization tasks"""
        
        self.query_analysis_prompt = PromptTemplate(
            input_variables=["query", "execution_plan", "performance_stats"],
            template="""
            As a senior database performance expert, analyze this SQL query and its performance characteristics:

            QUERY:
            {query}

            EXECUTION PLAN:
            {execution_plan}

            PERFORMANCE STATISTICS:
            {performance_stats}

            Provide a detailed analysis including:
            1. Primary performance bottlenecks
            2. Optimization opportunities 
            3. Recommended indexes
            4. Query rewrite suggestions
            5. Schema optimization recommendations

            Format your response as JSON with the following structure:
            {{
                "bottlenecks": ["list of identified bottlenecks"],
                "optimization_opportunities": ["list of optimization opportunities"],
                "index_suggestions": ["list of index recommendations"],
                "query_rewrites": ["list of query rewrite suggestions"],
                "schema_changes": ["list of schema optimization suggestions"],
                "confidence_score": 0.85
            }}
            """
        )
        
        self.index_recommendation_prompt = PromptTemplate(
            input_variables=["table_schema", "query_patterns", "current_indexes"],
            template="""
            As a database indexing expert, analyze the following table schema and query patterns to recommend optimal indexes:

            TABLE SCHEMA:
            {table_schema}

            CURRENT INDEXES:
            {current_indexes}

            QUERY PATTERNS:
            {query_patterns}

            Recommend the most impactful indexes considering:
            1. Query frequency and performance impact
            2. Index maintenance costs
            3. Storage overhead
            4. Composite index opportunities
            5. Partial index opportunities

            Return recommendations as JSON:
            {{
                "recommended_indexes": [
                    {{
                        "columns": ["col1", "col2"],
                        "type": "btree",
                        "partial_condition": "WHERE condition",
                        "estimated_improvement": 65.5,
                        "justification": "Reasoning for this index"
                    }}
                ],
                "indexes_to_drop": ["list of unused indexes"],
                "overall_confidence": 0.78
            }}
            """
        )

    def setup_tools(self):
        """Setup tools for the AI agent"""
        
        tools = [
            Tool(
                name="query_performance_analyzer",
                description="Analyze query performance statistics and execution plans",
                func=self._analyze_query_performance
            ),
            Tool(
                name="index_impact_calculator", 
                description="Calculate the potential impact of proposed indexes",
                func=self._calculate_index_impact
            ),
            Tool(
                name="schema_analyzer",
                description="Analyze table schemas and relationships for optimization opportunities",
                func=self._analyze_schema_structure
            ),
            Tool(
                name="cost_estimator",
                description="Estimate storage and computational costs of optimizations",
                func=self._estimate_optimization_costs
            )
        ]
        
        self.agent = initialize_agent(
            tools=tools,
            llm=self.llm,
            agent=AgentType.STRUCTURED_CHAT_ZERO_SHOT_REACT_DESCRIPTION,
            verbose=True,
            max_iterations=5
        )

    async def analyze_slow_queries(self, time_window_hours: int = 24) -> List[QueryAnalysis]:
        """
        Analyze slow queries from the performance log
        """
        logger.info(f"Analyzing slow queries from the last {time_window_hours} hours")
        
        async with get_async_db() as conn:
            # Get slow queries with aggregated statistics
            slow_queries = await conn.fetch("""
                SELECT 
                    query_hash,
                    query_text,
                    AVG(execution_time_ms) as avg_execution_time,
                    COUNT(*) as execution_count,
                    AVG(query_cost) as avg_cost,
                    MAX(execution_time_ms) as max_execution_time,
                    array_agg(DISTINCT execution_plan) as execution_plans
                FROM query_performance_log 
                WHERE timestamp >= NOW() - INTERVAL %s
                AND execution_time_ms > 1000  -- Only queries taking more than 1 second
                GROUP BY query_hash, query_text
                ORDER BY avg_execution_time DESC
                LIMIT 20
            """, f"{time_window_hours} hours")
            
            analyses = []
            for row in slow_queries:
                analysis = await self._deep_analyze_query(
                    query_hash=row['query_hash'],
                    query_text=row['query_text'],
                    avg_execution_time=row['avg_execution_time'],
                    execution_count=row['execution_count'],
                    cost_estimate=row['avg_cost'] or 0,
                    execution_plans=row['execution_plans']
                )
                analyses.append(analysis)
                
        logger.info(f"Completed analysis of {len(analyses)} slow queries")
        return analyses

    async def _deep_analyze_query(self, query_hash: str, query_text: str, 
                                 avg_execution_time: float, execution_count: int,
                                 cost_estimate: float, execution_plans: List) -> QueryAnalysis:
        """
        Perform deep analysis of a single query using AI
        """
        try:
            # Prepare performance statistics
            performance_stats = {
                "avg_execution_time_ms": avg_execution_time,
                "execution_count": execution_count,
                "cost_estimate": cost_estimate,
                "performance_category": self._categorize_performance(avg_execution_time)
            }
            
            # Use AI to analyze the query
            analysis_chain = LLMChain(llm=self.llm, prompt=self.query_analysis_prompt)
            result = await analysis_chain.arun(
                query=query_text,
                execution_plan=json.dumps(execution_plans[0] if execution_plans else {}),
                performance_stats=json.dumps(performance_stats)
            )
            
            # Parse AI response
            ai_analysis = json.loads(result)
            
            return QueryAnalysis(
                query_hash=query_hash,
                query_text=query_text,
                avg_execution_time=avg_execution_time,
                execution_count=execution_count,
                cost_estimate=cost_estimate,
                bottlenecks=ai_analysis.get('bottlenecks', []),
                optimization_opportunities=ai_analysis.get('optimization_opportunities', [])
            )
            
        except Exception as e:
            logger.error(f"Error analyzing query {query_hash}: {e}")
            # Fallback to rule-based analysis
            return self._fallback_query_analysis(query_hash, query_text, avg_execution_time, 
                                               execution_count, cost_estimate)

    def _categorize_performance(self, execution_time_ms: float) -> str:
        """Categorize query performance"""
        if execution_time_ms < 100:
            return "excellent"
        elif execution_time_ms < 500:
            return "good"
        elif execution_time_ms < 2000:
            return "acceptable"
        elif execution_time_ms < 10000:
            return "slow"
        else:
            return "critical"

    async def generate_optimization_suggestions(self, analyses: List[QueryAnalysis]) -> List[OptimizationSuggestion]:
        """
        Generate comprehensive optimization suggestions based on query analyses
        """
        logger.info(f"Generating optimization suggestions for {len(analyses)} queries")
        
        suggestions = []
        
        # Group queries by tables they access
        table_queries = self._group_queries_by_table(analyses)
        
        for table_name, table_analyses in table_queries.items():
            # Get table schema information
            schema_info = await self._get_table_schema(table_name)
            current_indexes = await self._get_current_indexes(table_name)
            
            # Generate index suggestions
            index_suggestions = await self._generate_index_suggestions(
                table_name, table_analyses, schema_info, current_indexes
            )
            suggestions.extend(index_suggestions)
            
            # Generate query rewrite suggestions
            rewrite_suggestions = await self._generate_query_rewrites(table_analyses)
            suggestions.extend(rewrite_suggestions)
            
            # Generate partitioning suggestions for large tables
            if await self._should_consider_partitioning(table_name):
                partition_suggestions = await self._generate_partitioning_suggestions(
                    table_name, table_analyses
                )
                suggestions.extend(partition_suggestions)
        
        # Store suggestions in database
        await self._store_suggestions(suggestions)
        
        logger.info(f"Generated {len(suggestions)} optimization suggestions")
        return suggestions

    def _group_queries_by_table(self, analyses: List[QueryAnalysis]) -> Dict[str, List[QueryAnalysis]]:
        """Group query analyses by the tables they primarily access"""
        table_queries = {}
        
        for analysis in analyses:
            # Extract table names from query (simple regex-based approach)
            # In production, would use a proper SQL parser
            tables = self._extract_table_names(analysis.query_text)
            
            for table in tables:
                if table not in table_queries:
                    table_queries[table] = []
                table_queries[table].append(analysis)
        
        return table_queries

    def _extract_table_names(self, query: str) -> List[str]:
        """Extract table names from SQL query"""
        import re
        # Simple regex to find table names after FROM and JOIN keywords
        # In production, would use a proper SQL parser like sqlparse
        pattern = r'(?:FROM|JOIN)\s+(\w+)'
        matches = re.findall(pattern, query, re.IGNORECASE)
        return [match.lower() for match in matches]

    async def _generate_index_suggestions(self, table_name: str, analyses: List[QueryAnalysis],
                                        schema_info: Dict, current_indexes: List[str]) -> List[OptimizationSuggestion]:
        """Generate intelligent index suggestions using AI"""
        
        query_patterns = [
            {
                "query": analysis.query_text,
                "execution_time": analysis.avg_execution_time,
                "frequency": analysis.execution_count,
                "bottlenecks": analysis.bottlenecks
            }
            for analysis in analyses[:5]  # Top 5 queries for this table
        ]
        
        try:
            index_chain = LLMChain(llm=self.llm, prompt=self.index_recommendation_prompt)
            result = await index_chain.arun(
                table_schema=json.dumps(schema_info),
                current_indexes=json.dumps(current_indexes),
                query_patterns=json.dumps(query_patterns)
            )
            
            ai_recommendations = json.loads(result)
            suggestions = []
            
            for rec in ai_recommendations.get('recommended_indexes', []):
                suggestion = OptimizationSuggestion(
                    type=OptimizationType.INDEX,
                    target_table=table_name,
                    description=f"Create index on {rec['columns']} for {table_name}",
                    implementation_sql=self._generate_index_sql(table_name, rec),
                    estimated_improvement=rec.get('estimated_improvement', 0),
                    confidence_score=ai_recommendations.get('overall_confidence', 0.5),
                    reasoning=rec.get('justification', 'AI-generated recommendation')
                )
                suggestions.append(suggestion)
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Error generating index suggestions for {table_name}: {e}")
            return []

    def _generate_index_sql(self, table_name: str, index_rec: Dict) -> str:
        """Generate CREATE INDEX SQL statement"""
        columns = ', '.join(index_rec['columns'])
        index_name = f"idx_{table_name}_{'_'.join(index_rec['columns'])}"
        
        sql = f"CREATE INDEX CONCURRENTLY {index_name} ON {table_name} ({columns})"
        
        if index_rec.get('partial_condition'):
            sql += f" {index_rec['partial_condition']}"
            
        return sql + ";"

    # Tool functions for the AI agent
    def _analyze_query_performance(self, query_info: str) -> str:
        """Tool function to analyze query performance"""
        # Implementation would analyze query execution plans, statistics, etc.
        return "Query analysis completed"

    def _calculate_index_impact(self, index_info: str) -> str:
        """Tool function to calculate index impact"""
        # Implementation would estimate the performance impact of proposed indexes
        return "Index impact calculated"

    def _analyze_schema_structure(self, table_name: str) -> str:
        """Tool function to analyze schema structure"""
        # Implementation would analyze table structure, relationships, etc.
        return "Schema analysis completed"

    def _estimate_optimization_costs(self, optimization_info: str) -> str:
        """Tool function to estimate costs"""
        # Implementation would estimate storage, computational costs
        return "Cost estimation completed"

    async def _get_table_schema(self, table_name: str) -> Dict:
        """Get table schema information"""
        # Implementation would query information_schema
        return {"columns": [], "constraints": [], "size": 0}

    async def _get_current_indexes(self, table_name: str) -> List[str]:
        """Get current indexes for a table"""
        # Implementation would query pg_indexes
        return []

    async def _should_consider_partitioning(self, table_name: str) -> bool:
        """Determine if table should be considered for partitioning"""
        # Implementation would check table size, growth rate, query patterns
        return False

    async def _generate_query_rewrites(self, analyses: List[QueryAnalysis]) -> List[OptimizationSuggestion]:
        """Generate query rewrite suggestions"""
        # Implementation would analyze queries and suggest rewrites
        return []

    async def _generate_partitioning_suggestions(self, table_name: str, analyses: List[QueryAnalysis]) -> List[OptimizationSuggestion]:
        """Generate table partitioning suggestions"""
        # Implementation would suggest partitioning strategies
        return []

    async def _store_suggestions(self, suggestions: List[OptimizationSuggestion]):
        """Store optimization suggestions in database"""
        async with get_async_db() as conn:
            for suggestion in suggestions:
                await conn.execute("""
                    INSERT INTO ai_optimization_suggestions 
                    (suggestion_type, target_table, suggestion_text, estimated_improvement,
                     implementation_sql, confidence_score, status)
                    VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
                """, suggestion.type.value, suggestion.target_table, suggestion.description,
                   suggestion.estimated_improvement, suggestion.implementation_sql, 
                   suggestion.confidence_score)

    def _fallback_query_analysis(self, query_hash: str, query_text: str, 
                                avg_execution_time: float, execution_count: int,
                                cost_estimate: float) -> QueryAnalysis:
        """Fallback rule-based analysis when AI fails"""
        bottlenecks = []
        opportunities = []
        
        if avg_execution_time > 5000:
            bottlenecks.append("Very slow execution time")
        if "SELECT *" in query_text.upper():
            opportunities.append("Avoid SELECT * statements")
        if "ORDER BY" in query_text.upper() and "LIMIT" not in query_text.upper():
            opportunities.append("Consider adding LIMIT to ORDER BY queries")
            
        return QueryAnalysis(
            query_hash=query_hash,
            query_text=query_text,
            avg_execution_time=avg_execution_time,
            execution_count=execution_count,
            cost_estimate=cost_estimate,
            bottlenecks=bottlenecks,
            optimization_opportunities=opportunities
        )
