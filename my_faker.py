"""

"""
import pandas as pd
from faker import Faker
from my_logs import Logs
import os
import logging
import boto3
from botocore.exceptions import ClientError
import sys


class DataFaker:
    def __init__(self, index):
        self.fake = Faker('zh_CN')
        self.data_dict = {}
        self.pt_profile = self.fake.profile()
        self.index = index
        Faker.seed(0)

    def employee_table(self):
        """
        创建员工表
        初步确定1000名员工
        其中领导40人
        :return:
        """
        # 员工人数
        data_size = 1000
        file_name = 'employee_table' + '.csv'
        file_path = './data/' + file_name
        if os.path.exists(file_path):
            os.remove(file_path)
            fake_data = pd.DataFrame(columns=('employee_id', 'name', 'login', 'create_time',
                                              'gender', 'level', 'state', 'bu_id', 'part_id',
                                              'city_id'))
            fake_data.to_csv(file_path, index=False, sep=',', header=True, encoding="utf_8_sig")
        else:
            fake_data = pd.DataFrame(columns=('employee_id', 'name', 'login', 'create_time',
                                              'gender', 'level', 'state', 'bu_id', 'part_id',
                                              'city_id'))
            fake_data.to_csv(file_path, index=False, sep=',', header=True, encoding="utf_8_sig")

        for i in range(data_size):
            employee_id = f"{i:08}"
            pt_profile = self.fake.profile()
            name = pt_profile['name']
            login = pt_profile['username']
            create_time = self.fake.date_between(start_date="-10y", end_date='-1y')
            gender = pt_profile['sex']
            level = self.fake.pyint(4, 6)
            state = 1
            bu_id = f"{self.fake.pyint(0, 10):05}"
            part_id = f"{self.fake.pyint(0, 2):05}"
            city_id = f"{self.fake.pyint(0, 10):05}"
            tmp_data = pd.DataFrame(
                {'employee_id': employee_id, 'name': name, 'login': login, 'create_time': create_time,
                 'gender': gender, 'level': level, 'state': state, 'bu_id': bu_id,
                 'part_id': part_id, 'city_id': city_id}, index=[0])

            tmp_data.to_csv(file_path, mode='a', index=False, sep=',', header=False, encoding="utf_8_sig")
        self.create_bucket('kervin-datalake-datademo')                                       
        self.upload_file(file_path, 'kervin-datalake-datademo', file_name)

    def level_tree_table(self):
        """
        上下级关系表
        初步确定1000名员工
        其中领导40人
        :return:
        """
        # hr_data = pd.read_csv('./data/employee_table.csv')
        file_name = 'level_tree_table' + '.csv'
        file_path = './data/' + file_name
        # 领导节点
        data_size = 1000
        fake_data = pd.DataFrame(columns=('employee_id', 'subordinate_id', 'subordinate_level'))
        for i in range(40):
            for j in range(25):
                employee_id = f"{i:08}"
                subordinate_id = f"{(j * 40 + 40 - 1):08}"
                subordinate_level = 2
                fake_data = fake_data.append(pd.DataFrame(
                    {'employee_id': employee_id, 'subordinate_id': subordinate_id,
                     'subordinate_level': subordinate_level}, index=[0]), ignore_index=True)

        # fake_data.to_csv('./data/' + 'employee_table' + '.csv',
        #                            index=False, sep=',', header=True, encoding="utf_8_sig")

        # 叶子节点
        for i in range(40, data_size):
            employee_id = f"{i:08}"
            subordinate_id = f"{i:08}"
            subordinate_level = 1
            fake_data = fake_data.append(pd.DataFrame(
                {'employee_id': employee_id, 'subordinate_id': subordinate_id,
                 'subordinate_level': subordinate_level}, index=[0]), ignore_index=True)

        fake_data.to_csv('./data/' + 'level_tree_table' + '.csv',
                         mode='a', index=False, sep=',', header=False, encoding="utf_8_sig")
        self.create_bucket('kervin-datalake-datademo')
        self.upload_file(file_path, 'kervin-datalake-datademo', file_name)

    def warehouse_table(self):
        """
        创建仓库表
        :return:
        """
        file_name = 'warehouse_table' + '.csv'
        file_path = './data/' + file_name
        data_size = 100
        fake_data = pd.DataFrame(columns=('warehouse_id', 'address', 'city_id', 'capacity',
                                          'max_capacity'))
        for i in range(data_size):
            warehouse_id = f"{self.fake.pyint(0, 9):05}"
            address = self.fake.address()
            city_id = f"{self.fake.pyint(0, 10):05}"
            max_capacity = self.fake.pyint(50, 100) * 1000
            capacity = self.fake.pyint(100, max_capacity)
            fake_data = fake_data.append(pd.DataFrame(
                {'warehouse_id': warehouse_id, 'address': address, 'city_id': city_id,
                 'capacity': capacity, 'max_capacity': max_capacity}, index=[0]), ignore_index=True)

        fake_data.to_csv('./data/' + 'warehouse_table' + '.csv',
                         mode='a', index=False, sep=',', header=False, encoding="utf_8_sig")
        self.create_bucket('kervin-datalake-datademo')
        self.upload_file(file_path, 'kervin-datalake-datademo', file_name)

    def customer_table(self, start, end):
        """
        创建客户表
        初步确定5000w名客户,4.8g
        :return:
        """
        # 客户人数
        file_name = 'customer_table' + self.index + '.csv'
        file_path = './data/' + file_name                                    
        if os.path.exists(file_path):
            os.remove(file_path)
            fake_data = pd.DataFrame(columns=('customer_id', 'customer_name', 'mail', 'address',
                                              'gender', 'create_time', 'city_id'))
            fake_data.to_csv(file_path, index=False, sep=',', header=True, encoding="utf_8_sig")
        else:
            fake_data = pd.DataFrame(columns=('customer_id', 'customer_name', 'mail', 'address',
                                              'gender', 'create_time', 'city_id'))
            fake_data.to_csv(file_path, index=False, sep=',', header=True, encoding="utf_8_sig")

        for i in range(start, end):
            customer_id = f"{i:010}"
            pt_profile = self.fake.profile()
            customer_name = pt_profile['name']
            mail = pt_profile['mail']
            create_time = self.fake.date_between(start_date="-9y")
            gender = pt_profile['sex']
            address = self.fake.address()
            city_id = f"{self.fake.pyint(0, 10):05}"
            tmp_data = pd.DataFrame(
                {'customer_id': customer_id, 'customer_name': customer_name, 'mail': mail, 'address': address,
                 'gender': gender, 'create_time': create_time,
                 'city_id': city_id}, index=[0])
            tmp_data.to_csv(file_path, mode='a', index=False, sep=',', header=False, encoding="utf_8_sig")

        self.create_bucket('kervin-datalake-datademo')
        self.upload_file(file_path, 'kervin-datalake-datademo', file_name)

    def sales_table(self, start, end):
        """
        创建销售表
        初步确定50000w条销售记录, 50g
        :return:
        """
        # 数据数量
        file_name = 'sales_table' + self.index + '.csv'
        file_path = './data/' + file_name
        if os.path.exists(file_path):
            os.remove(file_path)
            fake_data = pd.DataFrame(columns=('order_id', 'item_id', 'create_time', 'employee_id',
                                              'customer_id', 'city_id', 'buy_amt'))
            fake_data.to_csv(file_path, index=False, sep=',', header=True, encoding="utf_8_sig")
        else:
            fake_data = pd.DataFrame(columns=('order_id', 'item_id', 'create_time', 'employee_id',
                                              'customer_id', 'city_id', 'buy_amt'))
            fake_data.to_csv(file_path, index=False, sep=',', header=True, encoding="utf_8_sig")

        for i in range(start, end):
            order_id = f"{i:015}"
            employee_id = f"{self.fake.pyint(0, 1000):08}"
            create_time = self.fake.date_between(start_date="-9y")
            item_id = f"{i:015}"
            customer_id = f"{self.fake.pyint(0, 10000000):010}"
            city_id = f"{self.fake.pyint(0, 10):05}"
            buy_amt = self.fake.pyint(5000, 10000)
            tmp_data = pd.DataFrame(
                {'order_id': order_id, 'item_id': item_id, 'create_time': create_time, 'employee_id': employee_id,
                 'customer_id': customer_id, 'city_id': city_id,
                 'buy_amt': buy_amt}, index=[0])
            tmp_data.to_csv(file_path, mode='a', index=False, sep=',', header=False, encoding="utf_8_sig")
        self.create_bucket('kervin-datalake-datademo')
        self.upload_file(file_path, 'kervin-datalake-datademo', file_name)

    def product_view(self):
        """
        产品维表
        10个产品
        :return:
        """
        file_name = 'product_view' + '.csv'
        file_path = './data/' + file_name

        item_names = ['Pro_A', 'Pro_B', 'Pro_C', 'Pro_D', 'Pro_E', 'Pro_F', 'Pro_G', 'Pro_H', 'Pro_I', 'Pro_J']
        fake_data = pd.DataFrame(columns=('item_type_id', 'item_name', 'cost_amt'))
        for i in range(len(item_names)):
            item_type_id = f"{i:04}"
            item_name = item_names[i]
            cost_amt = self.fake.pyint(2000, 5000)
            fake_data = fake_data.append(pd.DataFrame(
                {'item_type_id': item_type_id, 'item_name': item_name,
                 'cost_amt': cost_amt}, index=[0]), ignore_index=True)

        fake_data.to_csv('./data/' + 'product_view' + '.csv',
                         mode='a', index=False, sep=',', header=False, encoding="utf_8_sig")
        self.create_bucket('kervin-datalake-datademo')
        self.upload_file(file_path, 'kervin-datalake-datademo', file_name)

    def product_table(self, start, end):
        """
        产品维表
        10个产品
        初步确定50000w条销售记录, 50g
        :return:
        """
        # 数据数量
        file_name = 'product_table' + self.index + '.csv'
        file_path = './data/' + file_name
        if os.path.exists(file_path):
            os.remove(file_path)
            fake_data = pd.DataFrame(columns=('item_id', 'create_time', 'warehouse_id',
                                              'exist_state', 'color_id', 'item_type_id'))
            fake_data.to_csv(file_path, index=False, sep=',', header=True, encoding="utf_8_sig")
        else:
            fake_data = pd.DataFrame(columns=('item_id', 'create_time', 'warehouse_id',
                                              'exist_state', 'color_id', 'item_type_id'))
            fake_data.to_csv(file_path, index=False, sep=',', header=True, encoding="utf_8_sig")

        for i in range(start, end):
            item_id = f"{i:015}"
            create_time = self.fake.date_between(start_date="-9y")
            warehouse_id = f"{self.fake.pyint(0, 9):05}"
            exist_state = self.fake.pyint(0, 1)
            item_type_id = f"{self.fake.pyint(0, 9):04}"
            tmp_data = pd.DataFrame(
                {'item_id': item_id, 'create_time': create_time, 'warehouse_id': warehouse_id,
                 'exist_state': exist_state,
                 'item_type_id': item_type_id}, index=[0])
            tmp_data.to_csv(file_path, mode='a', index=False, sep=',', header=False, encoding="utf_8_sig")
        self.create_bucket('kervin-datalake-datademo')
        self.upload_file(file_path, 'kervin-datalake-datademo', file_name)

    def city_table(self):
        file_name = 'city_table' + '.csv'
        file_path = './data/' + file_name
        city_name = ['北京', '上海', '广州', '深圳', '合肥', '成都', '武汉', '石家庄', '海口', '太原', '福建']
        city_id = ['00000', '00001', '00002', '00003', '00004', '00005', '00006', '00007', '00008', '00009', '00010']
        list_of_tuples = list(zip(city_id, city_name))
        fake_data = pd.DataFrame(list_of_tuples, columns=['city_id', 'city_name'])
        fake_data.to_csv('./data/' + 'city_table' + '.csv',
                         mode='a', index=False, sep=',', header=False, encoding="utf_8_sig")
        self.create_bucket('kervin-datalake-datademo')
        self.upload_file(file_path, 'kervin-datalake-datademo', file_name)

    def create_bucket(self, bucket_name, region=None):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                s3_client = boto3.client('s3')
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_file(self, file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True


if __name__ == '__main__':
    big_set = sys.argv[1]
    index = sys.argv[2]
    all_num = sys.argv[3]
    customer_num = sys.argv[4]
    faker = DataFaker(index)
    logger = Logs()
    logger.info("--version:0.0.2 datalake--")
    if big_set == '1':
        start_index = int(index) * int(int(all_num) / 10)
        end_index = (int(index) + 1) * int(int(all_num) / 10)
        logger.info("create Big_Set! from " + str(start_index) + " to " + str(end_index))
        logger.info("start fake customer_table data")
        customer_start_index = int(index) * int(int(customer_num) / 10)
        customer_end_index = (int(index) + 1) * int(int(customer_num) / 10)
        faker.customer_table(customer_start_index, customer_end_index)
        logger.info("finish fake customer_table data")
        logger.info("start fake product_table data")
        faker.product_table(start_index, end_index)
        logger.info("finish fake product_detial data")
        logger.info("start fake sales data")
        faker.sales_table(start_index, end_index)
        logger.info("finish fake sales data")
    else:
        logger.info("create Small_Set!")
        logger.info("start fake employee data")
        faker.employee_table()
        logger.info("finish fake employee data")
        logger.info("start fake level_tree data")

        faker.level_tree_table()
        logger.info("finish fake level_tree data")
        logger.info("start fake warehouse data")

        faker.warehouse_table()
        logger.info("finish fake warehouse data")
        logger.info("start fake city data")

        faker.city_table()
        logger.info("finish fake city data")
        logger.info("start fake customer_table data")

        faker.product_view()
        logger.info("finish fake product_view data")
