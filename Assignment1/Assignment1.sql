
/*
Creating the table and loading the dataset
*/
DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (userid INT, temp1 VARCHAR(10),  movieid INT , temp3 VARCHAR(10),  rating REAL, temp5 VARCHAR(10), timestamp INT);
COPY ratings FROM 'test_data.txt' DELIMITER ':';
ALTER TABLE ratings DROP COLUMN temp1, DROP COLUMN temp3, DROP COLUMN temp5, DROP COLUMN timestamp;

-- Do not change the above code except the path to the dataset.
-- make sure to change the path back to default provided path before you submit it.

-- Part A
/* Write the queries for Part A*/

-- Query 1
SELECT * FROM ratings WHERE userid=7;

-- Query 2
SELECT * FROM ratings WHERE userid=8 AND movieid=420;

-- Query 3
SELECT * FROM ratings WHERE movieid=733;

-- Query 4
SELECT * FROM ratings WHERE movieid=442 AND rating>2.5;

-- Query 5
SELECT * FROM ratings WHERE rating>4;

-- Part B
/* Create the fragmentations for Part B1 */

DROP TABLE IF EXISTS f1;

CREATE TABLE f1 AS

SELECT * FROM ratings WHERE rating>2 AND rating<=3.5;


DROP TABLE IF EXISTS f2;

CREATE TABLE f2 AS

SELECT * FROM ratings WHERE rating>=3;


DROP TABLE IF EXISTS f3;

CREATE TABLE f3 AS

SELECT * FROM ratings WHERE rating<=3;


/* Write reconstruction query/queries for Part B1 */

SELECT * FROM f1

UNION

SELECT * FROM f2

UNION

SELECT * FROM f3;


/* Write your explanation as a comment */

/* 
The fragment f1 above contains rows with rating 2.5, 3, 3.5.
The fragment f2 above contains rows with rating 3, 3.5, 4, 4.5, 5.
The fragment f3 above contains rows with rating 1, 1.5, 2, 2.5, 3.

So, we can see that there is an overlap between of rows between f1, f2, f3 fragments. So the above fragmentation does not satisfy disjointness property.
Performing an union operation will remove the overlapping rows and hence satisfies reconstruction and completeness properties.
So, the above fragments satisfies reconstruction and completeness but does not satisfy the disjointness property.
*/ 


/* Create the fragmentations for Part B2 */
DROP TABLE IF EXISTS f1;

CREATE TABLE f1 AS

SELECT movieid, rating FROM ratings WHERE rating<3;



DROP TABLE IF EXISTS f2;

CREATE TABLE f2 AS

SELECT userid, movieid FROM ratings;



DROP TABLE IF EXISTS f3;

CREATE TABLE f3 AS

SELECT movieid, rating FROM ratings WHERE rating>=3;

/* Write your explanation as a comment */
/*
The fragment f1 contains movieid, rating column of the ratings table with rating < 3. The primary key is movieid.
The fragment f2 contains userid, movieid column of the ratings table. The primary key is userid.
The fragment f3 contains movieid, rating column of the ratings table with rating >=3. The primary key is movieid.
The fragment f1, f2, f3 the recontruction property is not satisfies because there is no foreigh key in f1 and f3 corresponding to the primary key in f2.
The completness property is satisfied by the above fragments f1, f2, f3, since all the data is present in f1, f2, f3 when combines together.
Also, there is no overlap between the data of the above 3 fragments so the disjointness property is satisfied. 
*/

/* Create the fragmentations for Part B3 */
DROP TABLE IF EXISTS f1;

CREATE TABLE f1 AS

SELECT * FROM ratings WHERE rating<3;



DROP TABLE IF EXISTS f2;

CREATE TABLE f2 AS

SELECT * FROM ratings WHERE rating>2.5 AND rating<=4;



DROP TABLE IF EXISTS f3;

CREATE TABLE f3 AS

SELECT * FROM ratings WHERE rating>4;

/* Write reconstruction query/queries for Part B3 */
SELECT * FROM f1

UNION

SELECT * FROM f2

UNION

SELECT * FROM f3;

/* Write your explanation as a comment */
/*
The fragments f1, f2, and f3 satisfies all the 3 properties completeness, reconstruction, disjointness.
The fragments satisfies completeness property since no data is missing when all the fragments are combined together.
The fragments satisfies reconstruction because the union of all three fragments will finally result in the original data.
The fragments satisfies disjointness property since there is no overlap between the data of all three fragments.
*/

-- Part C
/* Write the queries for Part C */

-- Query 1

SELECT * FROM f1 WHERE userid=7

UNION

SELECT * FROM f2 WHERE userid=7

UNION

SELECT * FROM f3 WHERE userid=7;


-- Query 2

SELECT * FROM f1 WHERE userid=8 AND movieid=420

UNION

SELECT * FROM f2 WHERE userid=8 AND movieid=420

UNION

SELECT * FROM f3 WHERE userid=8 AND movieid=420;


-- Query 3

SELECT * FROM f1 WHERE movieid=733

UNION

SELECT * FROM f2 WHERE movieid=733

UNION

SELECT * FROM f3 WHERE movieid=733;


-- Query 4

SELECT * FROM f1 WHERE movieid=442 AND rating>2.5

UNION

SELECT * FROM f2 WHERE movieid=442 AND rating>2.5

UNION

SELECT * FROM f3 WHERE movieid=442 AND rating>2.5;


-- Query 5

SELECT * FROM f1 WHERE rating>4

UNION

SELECT * FROM f2 WHERE rating>4

UNION

SELECT * FROM f3 WHERE rating>4;