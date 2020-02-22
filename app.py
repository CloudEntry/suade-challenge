from flask import Flask, jsonify
# from flask_cors import CORS
import sqlite3

app = Flask(__name__)
# CORS(app)

# queries
# - The total number of items sold on that day.
total_items_sql = "select count(*) from orderline where order_id in (select id from orders where created_at like '<date>%');"

# - The total number of customers that made an order that day.
total_cust_sql = "select count(*) from (select distinct customer_id from orders where created_at like '<date>%');"

# - The total amount of discount given that day.
total_discount_sql = "select sum(discounted_amount) from orderline where order_id in (select id from orders where created_at like '<date>%');"

# - The average discount rate applied to the items sold that day.
avg_discount_sql = "select avg(discounted_amount) from orderline where order_id in (select id from orders where created_at like '<date>%');"

# - The average order total for that day
avg_total_sql = "select avg(total_amount) from orderline where order_id in (select id from orders where created_at like '<date>%');"

# - The total amount of commissions generated that day.
total_commission_sql = """
select sum(a.total_amount * c.rate)
from orderline as a
join orders as b on a.order_id = b.id
join vendorcommission as c on b.vendor_id = c.vendor_id
where b.created_at like '<date>%'
and c.date = '<date>';
"""

# - The average amount of commissions per order for that day.
average_commission_sql = """
select sum(a.total_amount * c.rate) / count(distinct b.id)
from orderline as a
join orders as b on a.order_id = b.id
join vendorcommission as c on b.vendor_id = c.vendor_id
where b.created_at like '<date>%'
and c.date = '<date>';
"""

# - The total amount of commissions earned per promotion that day.
total_commission_per_promotion_sql = """
select e.description, sum(a.total_amount * c.rate)
from orderline as a
join orders as b on a.order_id = b.id
join vendorcommission as c on b.vendor_id = c.vendor_id
join productpromotion as d on a.product_id = d.product_id
join promotion as e on d.promotion_id = e.id
where b.created_at like '<date>%'
and c.date = '<date>'
and d.date = '<date>'
group by d.promotion_id;
"""


@app.route('/api/v1.0/eshop/<date>', methods=['GET'])
def get_data(date):
    conn = sqlite3.connect('/home/suadechallenge/suade_data.db')
    c = conn.cursor()
    c.execute(total_items_sql.replace('<date>', date))
    total_items = c.fetchone()[0]
    c.execute(total_cust_sql.replace('<date>', date))
    total_cust = c.fetchone()[0]
    c.execute(total_discount_sql.replace('<date>', date))
    total_discount = c.fetchone()[0]
    c.execute(avg_discount_sql.replace('<date>', date))
    avg_discount = c.fetchone()[0]
    c.execute(avg_total_sql.replace('<date>', date))
    avg_total = c.fetchone()[0]
    c.execute(total_commission_sql.replace('<date>', date))
    total_commission = c.fetchone()[0]
    c.execute(average_commission_sql.replace('<date>', date))
    average_commission = c.fetchone()[0]

    data = {}
    data['items'] = total_items
    data['customers'] = total_cust
    data['total_discount_amount'] = total_discount
    data['discount_rate_avg'] = avg_discount
    data['order_average'] = avg_total
    data['total'] = total_commission

    commissions = {}
    commissions['average_commission'] = average_commission

    promotions = {}
    c.execute(total_commission_per_promotion_sql.replace('<date>', date))
    for row in c.fetchall():
        promotions[row[0]] = row[1]

    commissions['promotions'] = promotions
    data['commissions'] = commissions

    conn.close()
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
