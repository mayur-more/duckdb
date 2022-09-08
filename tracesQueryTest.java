package com.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class tracesQueryTest {

    public static void main(String args[]) throws ClassNotFoundException, SQLException {

        Runtime runtime = Runtime.getRuntime();
        long memory_s = runtime.totalMemory() - runtime.freeMemory();

        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt= conn.createStatement();

        long startTime = System.currentTimeMillis();

        String file = "/Users/mayurmore/Documents/TODO/176/S3Files/Raw_4.parquet";
//        String file = "s3://lm-apm-traces/d2eff1dab3354d89899b246d26272e22/2022-09-01/01/Raw_4.parquet";
        stmt.execute("INSTALL httpfs;");
        stmt.execute("LOAD httpfs;");
        stmt.execute("SET s3_region='us-west-2';");
        stmt.execute("SET s3_access_key_id='AKIASFKBB3MBQGMZNEL6';");
        stmt.execute("SET s3_secret_access_key='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';");


        String[] queries = {"SELECT startTime,error FROM read_parquet('"+ file +"') ORDER BY startTime DESC",
            "SELECT startTime,error FROM read_parquet('"+ file +"') ORDER BY startTime ASC",
            "SELECT startTime,error FROM read_parquet('"+ file +"') ORDER BY startTime DESC LIMIT 50",
            "SELECT startTime,error FROM read_parquet('"+ file +"') ORDER BY startTime ASC LIMIT 50",
            "SELECT startTime,error FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime DESC LIMIT 50",
            "SELECT startTime,error FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime ASC LIMIT 50",
            "SELECT duration,error,operation FROM read_parquet('"+ file +"')",
            "SELECT duration,error,operation FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7'",
            "SELECT spanId,traceId,startTime,error,operation FROM read_parquet('"+ file +"') ORDER BY startTime DESC LIMIT 500",
            "SELECT spanId,traceId,startTime,error,operation FROM read_parquet('"+ file +"') ORDER BY startTime ASC LIMIT 500",
            "SELECT spanId,traceId,startTime,error,operation FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime DESC LIMIT 500",
            "SELECT spanId,traceId,startTime,error,operation FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime ASC LIMIT 500",
            "select spanId,traceId,startTime,error,operation,duration FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration DESC LIMIT 50",
            "select spanId,traceId,startTime,error,operation,duration FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50",
            "select spanId,traceId,startTime,error,operation,duration FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration DESC LIMIT 500",
            "select spanId,traceId,startTime,error,operation,duration FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 500",
            "SELECT startTime,error,s3Tags FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY startTime ASC LIMIT 50",
            "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50",
            "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50 OFFSET 50*1",
            "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50 OFFSET 50*2",
            "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration ASC LIMIT 50 OFFSET 50*3",
            "select spanId,traceId,startTime,error,operation,duration,s3Tags FROM read_parquet('"+ file +"') WHERE companyUUID = 'f79c0ab73bf34b6688af13597728a7f7' ORDER BY duration DESC LIMIT 500",
            "select spanId,traceId,startTime,error,operation,duration,resource.name,resource.namespace FROM read_parquet('"+ file +"') WHERE resource.namespace = 'flipkart' LIMIT 50 ",
            "select s3Tags FROM read_parquet('"+ file +"') WHERE s3Tags ILIKE '%amazon%' LIMIT 50",
            "select startTime,s3Tags FROM read_parquet('"+ file +"') WHERE s3Tags ILIKE '%amazon%' ORDER BY startTime DESC LIMIT 50",
            "select startTime,s3Tags FROM read_parquet('"+ file +"') WHERE s3Tags ILIKE '%amazon%' ORDER BY startTime ASC LIMIT 50"};

        ResultSet rs;
        for(String s : queries) {
            System.out.println("\t Executing: - " + s);
            long startTime1 = System.currentTimeMillis();
            rs=stmt.executeQuery(s);
            System.out.println("\t Execution time: " + ((System.currentTimeMillis() - startTime1)) + " milliseconds");
            System.out.println();
        }

        System.out.println("Total Execution time: " + ((System.currentTimeMillis() - startTime)) + " milliseconds");

        long memory_e = runtime.totalMemory() - runtime.freeMemory();
        //System.out.println("Used memory is bytes: " + memory);
        System.out.println("Used memory is megabytes: "
            + (memory_e - memory_s)/(1024*2));

    }
}
