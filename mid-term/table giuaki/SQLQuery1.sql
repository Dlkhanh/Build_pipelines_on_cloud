select
s.name as schemaname,
t.name as tablename
from sys.tables t
inner join sys.schemas s
on t.schema_id=s.schema_id
where s.name='dbo'

@activity('Look for all tables').output.value
@{concat('select * from ', item().schemaname, '.', item().tablename)}
@item().schemaname
@item().tablename
@{concat(dataset().schemaname,'/',dataset().tablename)}               
@{concat(dataset().tablename,'.csv')}

SELECT 
    OI.order_ID,
    OI.order_item_ID,
    OI.item_ID,
    OI.discounted_price,
    OI.quantity,
    I.price
FROM 
    Order_Item OI
JOIN 
    Item I ON OI.item_ID = I.item_ID
WHERE 
    OI.discounted_price < I.price;




--XÓA DỮ LIỆU TRONG TẤT CẢ CÁC BẢNG
-- Disable foreign key constraints
ALTER TABLE Return_line_item NOCHECK CONSTRAINT ALL;
ALTER TABLE Returns NOCHECK CONSTRAINT ALL;
ALTER TABLE Payment NOCHECK CONSTRAINT ALL;
ALTER TABLE ORDER_STATUS NOCHECK CONSTRAINT ALL;
ALTER TABLE Order_Item NOCHECK CONSTRAINT ALL;
ALTER TABLE Customer_Order NOCHECK CONSTRAINT ALL;
ALTER TABLE Card NOCHECK CONSTRAINT ALL;
ALTER TABLE Review NOCHECK CONSTRAINT ALL;
ALTER TABLE Save_For_Later NOCHECK CONSTRAINT ALL;
ALTER TABLE Item NOCHECK CONSTRAINT ALL;
ALTER TABLE Account NOCHECK CONSTRAINT ALL;

-- Delete data from tables
DELETE FROM Return_line_item;
DELETE FROM Returns;
DELETE FROM Payment;
DELETE FROM ORDER_STATUS;
DELETE FROM Order_Item;
DELETE FROM Customer_Order;
DELETE FROM Card;
DELETE FROM Review;
DELETE FROM Save_For_Later;
DELETE FROM Item;
DELETE FROM Account;

-- Re-enable foreign key constraints
ALTER TABLE Return_line_item CHECK CONSTRAINT ALL;
ALTER TABLE Returns CHECK CONSTRAINT ALL;
ALTER TABLE Payment CHECK CONSTRAINT ALL;
ALTER TABLE ORDER_STATUS CHECK CONSTRAINT ALL;
ALTER TABLE Order_Item CHECK CONSTRAINT ALL;
ALTER TABLE Customer_Order CHECK CONSTRAINT ALL;
ALTER TABLE Card CHECK CONSTRAINT ALL;
ALTER TABLE Review CHECK CONSTRAINT ALL;
ALTER TABLE Save_For_Later CHECK CONSTRAINT ALL;
ALTER TABLE Item CHECK CONSTRAINT ALL;
ALTER TABLE Account CHECK CONSTRAINT ALL;
