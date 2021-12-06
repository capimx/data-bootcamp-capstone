DROP table user_behavior_metric;
create TABLE user_behavior_metric(
    customerid varchar(10),
    amount_spent DECIMAL(18,5),
    review_score INTEGER,
    review_count INTEGER,
    insert_date DATE
);

/* CREATE SCHEMA*/
create external schema movies_schema 
from data catalog 
database 'movies_db' 
iam_role 'arn:aws:iam::921884731971:role/redshift_role'
create external database if not exists;

/* TABLE FOR POSITIVE REVIEWS*/
CREATE EXTERNAL TABLE movies_schema.positive_reviews (
  cid             varchar(20),
  positive_review int
)
STORED AS parquet
LOCATION 's3://deb-silver/positive_reviews/'
TABLE PROPERTIES ('skip.header.line.count'='1');           

-- https://aws.amazon.com/blogs/aws/new-for-amazon-redshift-data-lake-export-and-federated-queries/
-- https://docs.aws.amazon.com/redshift/latest/dg/federated-create-secret-iam-role.html
-- https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html 

DROP SCHEMA IF EXISTS movie_purchases;

CREATE EXTERNAL SCHEMA IF NOT EXISTS movie_purchases
FROM POSTGRES
DATABASE 'dbname'
URI 'recreated-purchases.ca29galgbvzk.us-east-2.rds.amazonaws.com'
IAM_ROLE 'arn:aws:iam::921884731971:role/redshift-secrets'
SECRET_ARN 'arn:aws:secretsmanager:us-east-2:921884731971:secret:deb/replica_pwd-3MalvV';



-------------------Subquery 1----------------------------------------------
select reviews.cid, sum(reviews.positive_review) as review_score, count(reviews.positive_review) as review_count
from movies_schema.positive_reviews as reviews
WHERE reviews.cid IS NOT NULL
group by cid;

-------------------Subquery 2----------------------------------------------
select purchases.customerid, sum(purchases.quantity*purchases.unitprice) as amount_spent
from movie_purchases.user_purchases as purchases
where purchases.customerid IS NOT NULL
group by purchases.customerid;

-------------------Full select query----------------------------------------------
 WITH reviews_view AS
          (	SELECT reviews.cid, SUM(reviews.positive_review) AS review_score, COUNT(reviews.positive_review) AS review_count
          	FROM movies_schema.positive_reviews AS reviews
          	WHERE reviews.cid IS NOT NULL
          	GROUP BY cid), 
      purchases_view AS
          (	SELECT purchases.customerid, SUM(purchases.quantity*purchases.unitprice) AS amount_spent
            FROM movie_purchases.user_purchases AS purchases
            WHERE purchases.customerid IS NOT NULL
            AND LEN(purchases.customerid) > 0
            AND purchases.quantity > 0
            AND purchases.unitprice > 0
            GROUP BY purchases.customerid)
  SELECT cid, amount_spent, review_score, review_count,  CURRENT_DATE
  FROM reviews_view AS rv
  JOIN purchases_view AS pv ON rv.cid = pv.customerid;

-------------------Insert  query----------------------------------------------

INSERT INTO user_behavior_metric(
  WITH reviews_view AS
          (	SELECT reviews.cid, SUM(reviews.positive_review) AS review_score, COUNT(reviews.positive_review) AS review_count
          	FROM movies_schema.positive_reviews AS reviews
          	WHERE reviews.cid IS NOT NULL
          	GROUP BY cid), 
      purchases_view AS
          (	SELECT purchases.customerid, SUM(purchases.quantity*purchases.unitprice) AS amount_spent
            FROM movie_purchases.user_purchases AS purchases
            WHERE purchases.customerid IS NOT NULL
            AND LEN(purchases.customerid) > 0
            AND purchases.quantity > 0
            AND purchases.unitprice > 0
            GROUP BY purchases.customerid)
  SELECT cid, amount_spent, review_score, review_count,  CURRENT_DATE
  FROM reviews_view AS rv
  JOIN purchases_view AS pv ON rv.cid = pv.customerid);