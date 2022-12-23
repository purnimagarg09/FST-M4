-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/purnima/inputs' USING PigStorage('\t') AS (name:chararray, line:chararray);

--Filter out first 2 lines
ranked = RANK inputFile;
ranked_lines = FILTER ranked by (rank_inputFile > 2);

-- Group lines by name
grpd = GROUP ranked_lines BY name;

-- Count the number of lines by each character
total_count = FOREACH grpd GENERATE $0 as name, COUNT($1) as no_of_lines;
result = ORDER total_count BY no_of_lines DESC;

-- Remove output folder to rerun the script
-- rmf hdfs:///user/purnims/PigProjectOutput;

-- Store the result in HDFS
STORE result INTO 'hdfs:///user/purnima/PigProjectOutput';
