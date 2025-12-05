-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_carbon" to load this file. \quit

LOAD 'pg_carbon';
