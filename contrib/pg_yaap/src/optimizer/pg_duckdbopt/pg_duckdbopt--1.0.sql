/* contrib/pg_duckdbopt/pg_duckdbopt--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_duckdbopt" to load this file. \quit

-- The optimizer functionality is activated via hooks, no specific SQL functions needed usually.
-- You can define GUC toggles or helper functions here later.
