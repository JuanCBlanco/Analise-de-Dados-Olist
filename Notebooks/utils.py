import pandas as pd


def import_data():
    #  Importando os dados
    dataframes_dict = {
        "Customers": pd.read_csv("../Dados/olist_customers_dataset.csv"),
        "Geolocation": pd.read_csv("../Dados/olist_geolocation_dataset.csv"),
        "Order_Items": pd.read_csv("../Dados/olist_order_items_dataset.csv"),
        "Order_Payments": pd.read_csv("../Dados/olist_order_payments_dataset.csv"),
        "Order_Reviews": pd.read_csv("../Dados/olist_order_reviews_dataset.csv"),
        "Orders": pd.read_csv("../Dados/olist_orders_dataset.csv"),
        "Products": pd.read_csv("../Dados/olist_products_dataset.csv"),
        "Sellers": pd.read_csv("../Dados/olist_sellers_dataset.csv"),
    }

    #  Fazendo o tratamento do conjunto de dados dos Consumidores
    dataframes_dict["Customers"] = dataframes_dict["Customers"].assign(
        customer_state=dataframes_dict["Customers"].customer_state.astype("category")
    )

    #  Fazendo o tratamento do conjunto de dados dos Pedidos de Clientes
    dataframes_dict["Order_Items"] = dataframes_dict["Order_Items"].assign(
        shipping_limit_date=pd.to_datetime(
            dataframes_dict["Order_Items"]["shipping_limit_date"],
            format="%Y-%m-%d %H:%M:%S",
        ),
        total_cost=dataframes_dict["Order_Items"].price
        + dataframes_dict["Order_Items"].freight_value,
    )

    #  Fazendo o tratamento do conjunto de dados dos Tipo de Pagamentos dos Pedidos
    dataframes_dict["Order_Payments"] = dataframes_dict["Order_Payments"].assign(
        payment_type=dataframes_dict["Order_Payments"].payment_type.astype("category")
    )

    #  Fazendo o tratamento do conjunto de dados dos Comentários dos Pedidos
    dataframes_dict["Order_Reviews"] = dataframes_dict["Order_Reviews"].assign(
        review_creation_date=pd.to_datetime(
            dataframes_dict["Order_Reviews"]["review_creation_date"],
            format="%Y-%m-%d %H:%M:%S",
        ),
        review_answer_timestamp=pd.to_datetime(
            dataframes_dict["Order_Reviews"]["review_answer_timestamp"],
            format="%Y-%m-%d %H:%M:%S",
        ),
        reviews_reponse_time=lambda x: x.review_answer_timestamp
        - x.review_creation_date,
    )

    #  Fazendo o tratamento do conjunto de dados dos Pedidos
    dataframes_dict["Orders"] = (
        dataframes_dict["Orders"]
        .assign(
            order_purchase_timestamp=pd.to_datetime(
                dataframes_dict["Orders"]["order_purchase_timestamp"],
                format="%Y-%m-%d %H:%M:%S",
            ),
            order_approved_at=pd.to_datetime(
                dataframes_dict["Orders"]["order_approved_at"],
                format="%Y-%m-%d %H:%M:%S",
            ),
            order_delivered_carrier_date=pd.to_datetime(
                dataframes_dict["Orders"]["order_delivered_carrier_date"],
                format="%Y-%m-%d %H:%M:%S",
            ),
            order_delivered_customer_date=pd.to_datetime(
                dataframes_dict["Orders"]["order_delivered_customer_date"],
                format="%Y-%m-%d %H:%M:%S",
            ),
            order_estimated_delivery_date=pd.to_datetime(
                dataframes_dict["Orders"]["order_estimated_delivery_date"],
                format="%Y-%m-%d %H:%M:%S",
            ),
            order_status=dataframes_dict["Orders"].order_status.astype("category"),
            order_purchase_to_approved_time=lambda x: x.order_approved_at
            - x.order_purchase_timestamp,
            order_observed_delivery_time=lambda x: x.order_delivered_customer_date
            - x.order_purchase_timestamp,
        )[
            [
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
                "order_approved_at",
                "order_purchase_to_approved_time",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_observed_delivery_time",
                "order_estimated_delivery_date",
            ]
        ]
        .query(
            "not(order_approved_at.isnull() and order_status != 'canceled') and not(order_delivered_carrier_date.isnull() and order_status in ('approved', 'delivered'))"
        )
    )

    #  Fazendo o tratamento do conjunto de dados dos Vendedores
    dataframes_dict["Sellers"] = dataframes_dict["Sellers"].assign(
        seller_state=dataframes_dict["Sellers"].seller_state.astype("category")
    )

    return dataframes_dict
