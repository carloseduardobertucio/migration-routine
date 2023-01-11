import pandas
import json

from core.models.user import User
from core.models.product import Product
from core.models.sale import Sale
from core.repositories.user_repository import UserRepository
from core.repositories.product_repository import ProductRepository
from core.repositories.sale_repository import SaleRepository
from core.utils.configs import RoutineConfig


class MigrationRoutine:

    def __init__(self):
        self.source_config: dict = json.load(open("source_config.json"))
        self.config = RoutineConfig()

    def validate_column_pattern(self, dataframe, column_pattern):
        dataframe_columns = list(dataframe.columns)
        column_pattern = column_pattern.split(';')

        return dataframe_columns == column_pattern

    def read_csv_to_dataframe(self, path):
        try:
            dataframe = pandas.read_csv(path, encoding='ISO-8859-1', sep=';')
            return dataframe, len(dataframe.index) > 0

        except Exception as err:
            print(
                f"An error ocurred when trying to read csv file \n"
                f"Err: {err} "
            )
            return "", False

    def load_dataframe(self, file_name, column_pattern):

        file_path = f"./source/{file_name}.csv"
        dataframe, result = self.read_csv_to_dataframe(file_path)

        if not result:
            print(
                f"Was not possible read csv file in path: {file_path} \n"
                f"Is the name of the csv file correct as configured \n"
                f"in the config.ini file or the csv is populated?"
            )
            return "", False

        if not self.validate_column_pattern(dataframe, column_pattern):
            print(
                f"The column pattern [{column_pattern}] for [{file_name}] \n"
                f"Didn't match with the provided csv column names"
            )
            return "", False

        return dataframe, True

    def migrate_users(self):

        print("--- Starting the migration routine for users... ---")

        users_config = self.source_config.get("users_config")
        file_name = users_config.get("file_name")
        column_pattern = users_config.get("column_pattern")

        user_dataframe, result = self.load_dataframe(
            file_name=file_name,
            column_pattern=column_pattern
        )

        if not result:
            return

        user_repository = UserRepository()

        for row in user_dataframe.itertuples():
            new_user = User()
            new_user.load_from_pandas_obj(row)

            if not new_user:
                print(
                    f"User does not have the required data to be inserted. \n"
                    f"User: {new_user}"
                )
                continue

            print(f"Migrating user with email [{new_user.email}]...")

            user = user_repository.get_user(new_user.email)

            if user is None:
                continue

            if user:
                print(
                    f"User with email [{new_user.email}] already in the database and will not be migrated.")
                continue

            insert_result = user_repository.insert(new_user)

            if not insert_result:
                print(f"Couldn't insert user with email [{new_user.email}] successfully")
                continue

            print(f"User with email [{new_user.email}] migrated successfully.")

        print("--- Users migration routine ended. ---")

    def migrate_products(self):

        print("--- Starting the migration routine for products... ---")

        products_config = self.source_config.get("products_config")
        file_name = products_config.get("file_name")
        column_pattern = products_config.get("column_pattern")

        products_dataframe, result = self.load_dataframe(
            file_name=file_name,
            column_pattern=column_pattern
        )

        if not result:
            return

        product_repository = ProductRepository()

        for row in products_dataframe.itertuples():
            new_product = Product()
            new_product.load_from_pandas_obj(row)

            if not new_product:
                print(
                    f"Product does not have the required data to be inserted. \n"
                    f"Product: {new_product}"
                )
                continue

            print(f"Migrating product with name [{new_product.nome}]...")

            product = product_repository.get_product(new_product.nome)

            if product is None:
                continue

            if product:
                print(
                    f"Product with name [{new_product.nome}] already in the database and will not be migrated.")
                continue

            insert_result = product_repository.insert(new_product)

            if not insert_result:
                print(f"Couldn't insert product with name [{new_product.nome}] successfully")
                continue

            print(f"Product with name [{new_product.nome}] migrated successfully.")

        print("--- Products migration routine ended. ---")

    def migrate_sales(self):
        print("--- Starting the migration routine for sales... ---")

        sales_config = self.source_config.get("sales_config")
        file_name = sales_config.get("file_name")
        column_pattern = sales_config.get("column_pattern")

        sales_dataframe, result = self.load_dataframe(
            file_name=file_name,
            column_pattern=column_pattern
        )

        if not result:
            return

        user_repository = UserRepository()
        product_repository = ProductRepository()
        sales_repository = SaleRepository()

        for row in sales_dataframe.itertuples():
            new_sale = Sale()
            new_sale.load_from_pandas_obj(row)

            if not new_sale:
                print(
                    f"Sale does not have the required data to be inserted. \n"
                    f"Product: {new_sale}"
                )

            print(
                f"Migrating sale for \n"
                f"User [{new_sale.email_usuario}] \n"
                f"Product [{new_sale.produto}]"
            )

            user = user_repository.get_user(new_sale.email_usuario)
            product = product_repository.get_product(new_sale.produto)

            if not user:
                print(
                    f"User with email [{new_sale.email_usuario}] doesn't exist in the database and will not be migrated.")
                continue

            if not product:
                print(
                    f"Product with name [{new_sale.produto}] doesn't exist in the database and will not be migrated.")
                continue

            new_sale.id_usuario = user.id_usuario
            new_sale.id_produto = product.id_produto

            insert_result = sales_repository.insert(new_sale)

            if not insert_result:
                print(f"Couldn't insert sale successfully")
                continue

            print(f"Sale inserted succesfully.")

        print("--- Sales migration routine ended. ---")

    def execute(self):

        print('----- Migration Routines Started -----')

        if self.config.migrate_users:
            self.migrate_users()

        if self.config.migrate_products:
            self.migrate_products()

        if self.config.migrate_sales:
            self.migrate_sales()

        print('----- End of Migrations Routine -----')


if __name__ == '__main__':
    routine = MigrationRoutine()
    routine.execute()
