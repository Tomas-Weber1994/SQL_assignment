--- Point to parts that could be optimized
--- Feel free to comment any row that you think could be optimize/adjusted in some way!
--- The following query is from SAP HANA but applies to any DB
--- Do not worry if the tables/columns are not familiar to you 
----   -> you do not need to interpret the result (in fact the query does not reflect actual DB content)

--- My comments: ---

/* For better performance I would try to index predicates in JOIN and WHERE clauses (in rows containing ORDER BY and GROUP BY also, but these clauses are not used), e. g.:
 * 
CREATE INDEX index_RSEG_MANDT 
ON "DTAG_DEV_CSBI_CELONIS_DATA"."dtag.dev.csbi.celonis.data.elog::V_RSEG"(MANDT) 

* Also, before joining tables I would try to restrict them to a minimum size (only needed columns, WHERE clause already applied), 
* therefore I would at first create temporary help tables using WITH statement
* 
*/

--- Other comments lower within the script  ---

SELECT 
	RSEG.EBELN, 
	/* Column names are not clear and readable, e. g. instead of BKPF.GJAHR I would prefer BKPF.Geburtsjahr (If correct), 
	 * by BSCHL or SHZKG I have no idea what the column is about -   it should be always clear what are we selecting */
	RSEG.EBELP,
    RSEG.BELNR,
    RSEG.AUGBL AS AUGBL_W,
    LPAD(EKPO.BSART,6,0) as BSART, /* When using SQL operators, i would always use capitalizing - AS */
	BKPF.GJAHR,
	BSEG.BUKRS,
	BSEG.BUZEI,
	BSEG.BSCHL,
	BSEG.SHKZG,
    CASE WHEN BSEG.SHKZG = 'H' THEN (-1) * BSEG.DMBTR ELSE BSEG.DMBTR END AS DMBTR,
    COALESCE(BSEG.AUFNR, 'Kein SM-A Zuordnung') AS AUFNR,
    COALESCE(LFA1.LAND1, 'Andere') AS LAND1, 
    LFA1.LIFNR,
    LFA1.ZSYSNAME,
    BKPF.BLART as BLART,
    BKPF.BUDAT as BUDAT,
    BKPF.CPUDT as CPUDT
FROM "DTAG_DEV_CSBI_CELONIS_DATA"."dtag.dev.csbi.celonis.data.elog::V_RSEG" AS RSEG /* FROM (SELECT <needed_columns> FROM <database_name>.<table_name> WHERE MANDT = '200') AS RSEG */
LEFT JOIN "DTAG_DEV_CSBI_CELONIS_WORK"."dtag.dev.csbi.celonis.app.p2p_elog::__P2P_REF_CASES" AS EKPO ON 1=1 /* Is LEFT JOIN really needed? Theoretically INNER JOIN should be faster */
    AND RSEG.ZSYSNAME = EKPO.SOURCE_SYSTEM
    AND RSEG.MANDT = EKPO.MANDT /* RSEG.MANDT must be '200' -> EKPO table can be also restricted before joining and this row would be after that useless */
    AND RSEG.EBELN || RSEG.EBELP = EKPO.EBELN || EKPO.EBELP /* Concatenation is on this place not appropriate */
INNER JOIN "DTAG_DEV_CSBI_CELONIS_DATA"."dtag.dev.csbi.celonis.data.elog::V_BKPF" AS BKPF ON 1=1
    AND BKPF.AWKEY = RSEG.AWKEY /* RSEG.AWKEY = BKPF.AWKEY (higher readability) */
    AND RSEG.ZSYSNAME = BKPF.ZSYSNAME
    AND RSEG.MANDT in ('200') /* Condition is on my openion on the wrong place, 
    RSEG table should be restricted (using WHERE clause) before joining other tables, see comment above in the FROM clause*/
INNER JOIN "DTAG_DEV_CSBI_CELONIS_DATA"."dtag.dev.csbi.celonis.data.elog::V_BSEG" AS BSEG ON 1=1 
    AND DATS_IS_VALID(BSEG.ZFBDT) = 1 /* Is the function needed? Lowering performance --> solution e. g. WHERE BSEG.ZFBDT BETWEEN ‘7/1/2018’ AND ‘7/30/2021’ (only example)
    /* I would try to avoid using functions when joining tables - And I would also create at first a temporary table, which I would join afterwards: 
     * WITH temporary_table AS (
     * 	SELECT 
     * 		<needed_columns>
     * FROM DTAG_DEV_CSBI_CELONIS_DATA"."dtag.dev.csbi.celonis.data.elog::V_BSEG" AS BSEG
     * WHERE 
     * 		DATS_IS_VALID(BSEG.ZFBDT) = 1 AND (better would be to rewrite it without function (example above), if the predicate would be indexed, the index would have no impact because of the function)
     * 		BSEG.KOART = 'K' AND
     * 		CAST(BSEG.GJAHR AS INT) = 2020 (again, the best would be to rewrite it without the function, as this is only a conversion, I am not sure if the CAST function is on this place needed at all)
     * 		)
     */
    AND BSEG.KOART = 'K' 
    AND CAST(BSEG.GJAHR AS INT) = 2020 /* without function --> AND BSEG.GJAHR = '2020' (if string before) */
    /* Also, these two conditions above I would shift into the WHERE clause during temporary table creation as above */
    AND BKPF.ZSYSNAME = BSEG.ZSYSNAME
    AND BKPF.MANDT = BSEG.MANDT 
    /* I am asking myself if it is needed to repeat the MANDT condition so many times, if RSEG.MANDT = '200', 
     * but I can´t see any explicit connection between RSEG.MANDT and BKPF or BSEG.MANDT */
    AND BKPF.BUKRS = BSEG.BUKRS
    AND BKPF.GJAHR = BSEG.GJAHR
    AND BKPF.BELNR = BSEG.BELNR
    AND BSEG.DMBTR*-1 >= 0 /* AND BSEG.DMBTR*-1 <= 0, it is enough to reverse the logic instead of using *-1 */
INNER JOIN (SELECT * FROM "DTAG_DEV_CSBI_CELONIS_DATA"."dtag.dev.csbi.celonis.data.elog::V_LFA1" AS TEMP 
            WHERE TEMP.LIFNR > '020000000') AS LFA1 ON 1=1 /* Avoid using SELECT * if only a few columns are valuable for our purposes */
    AND BSEG.ZSYSNAME = LFA1.ZSYSNAME
    AND BSEG.LIFNR=LFA1.LIFNR
    AND BSEG.MANDT=LFA1.MANDT
    AND LFA1.LAND1 in ('DE','SK') /* Such condition should be in the WHERE clause, best option is on my openion to restrict the table before joining:
     * I would create again a temporary table containing only valuable information for our purposes + using UNION instead of LFA1.LAND1 in ('DE','SK') - should have better performance:
     * WITH temporary_table_2 AS (
     * 	SELECT 
     * 		<needed_columns> (not *)
     * 	FROM "DTAG_DEV_CSBI_CELONIS_DATA"."dtag.dev.csbi.celonis.data.elog::V_LFA1" 
        WHERE 
        	LIFNR > '020000000' AND
        	LAND1 = 'DE'
        UNION
        SELECT 
     * 		<needed_columns>
     * 	FROM "DTAG_DEV_CSBI_CELONIS_DATA"."dtag.dev.csbi.celonis.data.elog::V_LFA1" 
        WHERE 
        	LIFNR > '020000000' AND
        	LAND1 = 'SK'        
        )
        
     * LFA1.LAND1 IN ('DE, 'SK') is shorter but the performance should be theoretically lower in comparison with UNION operator, which would also remove potential duplicates
     */
    
    