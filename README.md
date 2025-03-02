# SQL Filters Project

This project is a simple implementation of SQL filters.

## Transactions Data Overview
This table contains transaction details including the transaction ID, date, amount, category, product, and payment method.

# Total Transactions Details
![image](https://github.com/user-attachments/assets/6b35d5ce-cab3-4490-ae35-76fc44668205)

# Single column data
![image](https://github.com/user-attachments/assets/35f711ec-1965-458a-a208-f6afedebe59b)


# Multip cloumn data
![image](https://github.com/user-attachments/assets/57f53c17-d0ab-4ac8-889d-98bca42035e8)

# Null Data
![image](https://github.com/user-attachments/assets/47990b3f-215a-4c1d-a9d3-05bd42adacb4)

# Max ID
![image](https://github.com/user-attachments/assets/41cd25d0-92c2-44fe-a1f5-8f3d4c50e939)

# Total rows of Count
![image](https://github.com/user-attachments/assets/d182674a-acf3-4f72-897c-665a10265c49)

# Status of payments if cash show green other show blue
![image](https://github.com/user-attachments/assets/c022c8d9-6284-4208-a5c3-54ffb64923b0)
![image](https://github.com/user-attachments/assets/145e9de2-3b0c-4ab6-aa5c-a2941444d215)
![image](https://github.com/user-attachments/assets/2451c7ca-08fe-41dc-913e-377256020715)

# Concat two columns
![image](https://github.com/user-attachments/assets/712c4192-40ca-419a-8f51-adc3ebd3439e)
![image](https://github.com/user-attachments/assets/1734f641-4f75-4e13-a0c3-0d157bb324e7)
![image](https://github.com/user-attachments/assets/45c60701-16d5-49a1-9d84-416f8030cab8)

# lower case
![image](https://github.com/user-attachments/assets/879904c6-8952-41fb-8bb2-81aaf3025520)

# UPPER CASE 
![image](https://github.com/user-attachments/assets/11ef8a16-7c7d-413f-a75b-d499c62e8dc4)

# CEIL operation
![image](https://github.com/user-attachments/assets/e0ec3b65-d93f-4942-bccf-5ada3dc950d2)

# Round operation
![image](https://github.com/user-attachments/assets/e22b0363-6de9-4102-85f9-efae181f7785)

# Replace Nulls
![image](https://github.com/user-attachments/assets/1276ffe8-38d4-47b8-9652-5a89db545a1e)

# Trim Space
![image](https://github.com/user-attachments/assets/fdb2af13-559d-4ffb-92c2-59efd6e99180)

# Distinct & Spendbby
![image](https://github.com/user-attachments/assets/0c6ab049-6efb-4a18-aed7-298010af4397)
![image](https://github.com/user-attachments/assets/5f06b5ae-ece3-44ec-803f-d7cf0095d8d2)

# Substring
![image](https://github.com/user-attachments/assets/4c0b22f9-caab-4d9c-88a0-9fa198557fa4)

# Split
![image](https://github.com/user-attachments/assets/d6ca108f-411b-4f43-bcf8-b981b44ac167)

# UNION
# <-- spark.sql("select * from df union all select * from df1").show() -->
![image](https://github.com/user-attachments/assets/b7dabde5-472d-45cb-82a1-050be3ed9c99)
![image](https://github.com/user-attachments/assets/144b3115-71e4-4297-84c0-3c7a053b1f19)

# Total amount col for Each catogery
# <-- spark.sql("select category,sum(amount) as sum from df group by category").show() -->
![image](https://github.com/user-attachments/assets/4b8c30c7-7900-4ea3-8926-ddfd7cf0e51c)

# Total amount category spendby
# <-- spark.sql("select category,spendby,sum(amount) as sum from df group by category,spendby").show() -->
![image](https://github.com/user-attachments/assets/fd746ae4-73b6-4d18-8861-e8f25f2e35d7)
# <-- spark.sql("select category,spendby,sum(amount) as sum,count(amount) as count from df group by category,spendby").show() -->
![image](https://github.com/user-attachments/assets/031d40ff-6d85-4837-a938-0794735a12fd)

# #what is MAX amount of every category
# <-- spark.sql("select category, max(amount) as MAX from df group by category").show() -->
![image](https://github.com/user-attachments/assets/e35c06cf-66c5-4e91-a9b0-3feedac273e6)
# <-- spark.sql("select category, max(amount) as MAX from df group by category order by category").show() -->
![image](https://github.com/user-attachments/assets/57297524-989d-4145-b456-106d57edfc69)

# <-- spark.sql("select category, max(amount) as MAX from df group by category order by category desc").show() -->
![image](https://github.com/user-attachments/assets/563cd221-c79f-43fa-ac33-bfaea377c3a7)

# Window Row Number
# <-- spark.sql("SELECT category, amount, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS row_number FROM df").show() -->
![image](https://github.com/user-attachments/assets/b4fcf820-db4b-4b31-b66f-0b52139738f6)

# <-- spark.sql("SELECT category, amount, rank() OVER (PARTITION BY category ORDER BY amount DESC) AS row_number FROM df").show() -->
![image](https://github.com/user-attachments/assets/54d2232f-e165-4401-90f1-b96c96cf05b5)

# Window lead function
# <-- spark.sql("select category, amount, lead(amount) over (partition by category order by amount desc) as lead from df").show() -->
![image](https://github.com/user-attachments/assets/caf8fe7e-d635-4855-b315-d46d6244b7ed)
# Lag function
# <-- spark.sql("select category, amount, lag(amount) over (partition by category order by amount desc) as lag from df").show() -->
![image](https://github.com/user-attachments/assets/da1910b0-3319-48d4-a261-82f813fd94d4)

# Having function
# <-- spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show() -->
![image](https://github.com/user-attachments/assets/f8e89bee-cc4a-4147-80fa-41dff43f3e93)

# Join
# <-- spark.sql("SELECT a.*,b.product FROM df a join df1 b on a.id=b.id").show() -->
![image](https://github.com/user-attachments/assets/a0b2624e-0c67-4cc0-9d8e-f07029e6075e)

 
# left Join
# <-- spark.sql("SELECT a.*,b.product FROM df a left join df1 b on a.id=b.id").show() -->

 ![image](https://github.com/user-attachments/assets/bb69748c-2375-4026-a73d-f5d4401d24c4)


# Right Join
# <<-- spark.sql("SELECT a.*,b.product FROM cust a right join prod b on a.id=b.id").show() -->
![image](https://github.com/user-attachments/assets/3c4e22a1-fb56-4ac7-bf73-96f3b536f9a6)

 
























