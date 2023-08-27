import unittest
from pyspark.sql import SparkSession
from etl_utils import *

class TestETL(unittest.TestCase):
  def setUp(self):
    self.spark = get_spark_session()

  def tearDown(self):
    self.spark.stop()

  def read_csv_data(self):
    input_path = "/opt/data/transaction.csv"
    df = read_psv(self.spark, input_path).withColumnRenamed("custId", "customer_id")
    df.cache()
    return df

  # Unit test favorite product 
  def test_favorite_product_and_longest_streak(self):
    # Expected favorite products for customers
    fav_prod_dict = {
      "0024003" : "SUPA101",
      "0023824" : "PURA250",
      "0024212" : "PURA100",
      "0023510" : "PURA100"
    }
    # Expected longest streak for customers
    streak_dict = {
      "0024003" : 3,
      "0023824" : 3,
      "0024212" : 2,
      "0023510" : 1
    }
    
    # Read CSV input and compute ETL output
    df = self.read_csv_data().filter(
        col("customer_id").isin(list(fav_prod_dict.keys()))
      )
    etl_output = get_etl_output(
        get_favourite_product(df),
        get_longest_streak(df)
      )
    data = etl_output.collect()
    result_dict = {row["customer_id"]: [row["favorite_product"], row["longest_streak"]] for row in data}
    
    assert len(data) == len(fav_prod_dict.keys()), f"ETL output has missing customer_ids!"

    for cust_id in fav_prod_dict.keys():
      prod_id = result_dict.get(cust_id)[0]
      streak = result_dict.get(cust_id)[1]

      with self.subTest(cust_id):
        assert prod_id == fav_prod_dict.get(cust_id), f"Favorite product  test failed for [{cust_id}]. Expected [{fav_prod_dict.get(cust_id)}], actual: [{prod_id}]"
        print(f">>>> Favorite Product [{cust_id}, {prod_id}] is VALID!")

      with self.subTest(cust_id):
        assert streak == streak_dict.get(cust_id), f"Favorite product  test failed for [{cust_id}]. Expected [{expected_longest_streak}], actual: [{streak}]"
        print(f">>>> Streak [{cust_id}, {streak}] is VALID!")

  # # Unit test favorite product 
  # def test_favorite_product(self):
  #   # Expected favorite products for customers
  #   fav_prod_dict = {
  #     "0024003" : "SUPA101",
  #     "0023824" : "PURA250",
  #     "0023855" : "PURA100",
  #     "0024221" : "PURA100"
  #   }

  #   # Compute favorite products
  #   df = self.read_csv_data().filter(col("customer_id").isin(list(fav_prod_dict.keys())))
  #   fav_prod_df = get_favourite_product(df)
  #   data = fav_prod_df.collect()
  #   result_dict = {row["customer_id"]: row["favorite_product"] for row in data}
    
  #   # Tetsing computed favorite products against expected
  #   for cust_id, expected_fav_product in fav_prod_dict.items():
  #     with self.subTest(cust_id=cust_id):
  #       self.assertEqual(
  #         result_dict.get(cust_id), 
  #         expected_fav_product, 
  #         f"Longest streak test failed forr customer_id: [{cust_id}]. Expected: [{expected_fav_product}], actual: [{result_dict.get(cust_id)}]."
  #       )

  # # Unit test longest streak
  # def test_longest_streak(self):
  #   # Expected longest streak for customers
  #   streak_dict = {
  #     "0024003" : 3,
  #     "0023824" : 3,
  #     "0024024" : 2,
  #     "0023658" : 2,
  #     "0023624" : 1
  #   }

  #   # Compute longest streak
  #   df = self.read_csv_data().filter(col("customer_id").isin(list(streak_dict.keys())))
  #   longest_streak_df = get_longest_streak(df)
  #   data = longest_streak_df.collect()
  #   result_dict = {row["customer_id"]: row["longest_streak"] for row in data}

  #   # Tetsing computed favorite products against expected
  #   for cust_id, expected_longest_streak in streak_dict.items():
  #     with self.subTest(cust_id=cust_id):
  #       self.assertEqual(
  #         result_dict.get(cust_id), 
  #         expected_longest_streak, 
  #         f"Longest streak test failed forr customer_id: [{cust_id}]. Expected: [{expected_longest_streak}], actual: [{result_dict.get(cust_id)}]."
  #       )

  def test_check_specific_customer(self):
    # Specifications
    cust_id = "0023938"
    expected_fav_product = "PURA250"
    expected_longest_streak = 2

    # Read CSV input and compute ETL output
    df = self.read_csv_data().filter(
        col("customer_id") == cust_id
      )
    etl_output = get_etl_output(
        get_favourite_product(df),
        get_longest_streak(df)
      ).select(
        "favorite_product", 
        "longest_streak"
      ).limit(1).collect()
    
    favourite_product = etl_output[0][0]
    longest_streak = etl_output[0][1]

    with self.subTest():
      assert favourite_product == expected_fav_product, f"Favorite product  test failed for [{cust_id}]. Expected [{expected_fav_product}], actual: [{favourite_product}]"
      print(">>>> Favorite Product is VALID!")

    with self.subTest():
      assert longest_streak == expected_longest_streak, f"Favorite product  test failed for [{cust_id}]. Expected [{expected_longest_streak}], actual: [{longest_streak}]"
      print(">>>> Streak is VALID!")
    

if __name__ == "__main__":
  unittest.main()
