import pandas as pd
from faker import Faker
import boto3
from boto3.session import Session

class DataFaker:
    def __init__(self):
        self.fake = Faker('zh_CN')
        self.data_dict = {}
        self.pt_profile = self.fake.profile()
        self.session = Session("您的AccessKey", "您的SecretKey")
        self.s3_client = Session.client('s3', endpoint_url="数据中心Endpoint")
        Faker.seed(0)

    def fake_data(self):
        pt_profile = self.fake.profile()
        self.data_dict['FPATNO'] = f"{self.fake.pyint(15000, 17000):05}"
        self.data_dict['ZYH'] = f"{self.fake.pyint(200, 350):05}"
        self.data_dict['name'] = pt_profile['name']
        self.data_dict['gender'] = pt_profile['sex']
        self.data_dict['age'] = self.fake.pyint(15, 50)
        self.data_dict['ruyuan'] = self.fake.past_datetime(start_date="-30d", tzinfo=None)
        # self.data_dict['chuyuan'] = self.fake.past_datetime(start_date=self.data_dict['ruyuan'] + timedelta(days=1),
        #                                                tzinfo=None)
        print(self.data_dict)

    def employee_table(self):
        """
        创建员工表
        初步确定1000名员工
        其中领导40人
        :return:
        """
        # 员工人数
        data_size = 1000
        fake_data = pd.DataFrame(columns=('employee_id', 'name', 'login', 'create_time',
                                          'gender', 'level', 'state', 'bu_id', 'part_id',
                                          'city_id'))
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
            fake_data = fake_data.append(pd.DataFrame(
                {'employee_id': employee_id, 'name': name, 'login': login, 'create_time': create_time,
                 'gender': gender, 'level': level, 'state': state, 'bu_id': bu_id,
                 'part_id': part_id, 'city_id': city_id}, index=[0]), ignore_index=True)

        fake_data.to_csv('./data/' + 'employee_table' + '.csv',
                         index=False, sep=',', header=True, encoding="utf_8_sig")

    def level_tree_table(self):
        """
        上下级关系表
        初步确定1000名员工
        其中领导40人
        :return:
        """
        hr_data = pd.read_csv('./data/employee_table.csv')
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
                         index=False, sep=',', header=True, encoding="utf_8_sig")

    def warehouse_table(self):
        """
        创建仓库表
        :return:
        """
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
                         index=False, sep=',', header=True, encoding="utf_8_sig")

    def customer_table(self):
        """
        创建客户表
        初步确定10w名客户
        :return:
        """
        # 客户人数
        data_size = 1000
        fake_data = pd.DataFrame(columns=('customer_id', 'customer_name', 'mail', 'address',
                                          'gender', 'create_time', 'city_id'))
        for i in range(data_size):
            customer_id = f"{i:010}"
            pt_profile = self.fake.profile()
            customer_name = pt_profile['name']
            mail = pt_profile['mail']
            create_time = self.fake.date_between(start_date="-9y")
            gender = pt_profile['sex']
            address = self.fake.address()
            city_id = f"{self.fake.pyint(0, 10):05}"
            fake_data = fake_data.append(pd.DataFrame(
                {'customer_id': customer_id, 'customer_name': customer_name, 'mail': mail, 'address': address,
                 'gender': gender, 'create_time': create_time,
                 'city_id': city_id}, index=[0]), ignore_index=True)

        fake_data.to_csv('./data/' + 'customer_table' + '.csv',
                         index=False, sep=',', header=True, encoding="utf_8_sig")

    def sales_table(self):
        """
        创建销售表
        初步确定1000w条销售记录
        :return:
        """
        # 数据数量
        data_size = 1000
        fake_data = pd.DataFrame(columns=('order_id', 'item_id', 'create_time', 'employee_id',
                                          'customer_id', 'city_id', 'buy_amt'))
        for i in range(data_size):
            order_id = f"{i:015}"
            employee_id = f"{self.fake.pyint(0, 1000):08}"
            create_time = self.fake.date_between(start_date="-9y")
            item_id = f"{i:015}"
            customer_id = f"{self.fake.pyint(0, 10000000):010}"
            city_id = f"{self.fake.pyint(0, 10):05}"
            buy_amt = self.fake.pyint(5000, 10000)
            fake_data = fake_data.append(pd.DataFrame(
                {'order_id': order_id, 'item_id': item_id, 'create_time': create_time, 'employee_id': employee_id,
                 'customer_id': customer_id, 'city_id': city_id,
                 'buy_amt': buy_amt}, index=[0]), ignore_index=True)

        fake_data.to_csv('./data/' + 'sales_table' + '.csv',
                         index=False, sep=',', header=True, encoding="utf_8_sig")

    def product_view(self):
        """
        产品维表
        10个产品
        :return:
        """
        # 数据数量
        data_size = 10
        item_names = ['Pro_A', 'Pro_B', 'Pro_C', 'Pro_D', 'Pro_E', 'Pro_F', 'Pro_G', 'Pro_H', 'Pro_I', 'Pro_J']
        fake_data = pd.DataFrame(columns=('item_type_id', 'item_name', 'cost_amt'))
        for i in range(data_size):
            item_type_id = f"{i:04}"
            item_name = item_names[i]
            cost_amt = self.fake.pyint(2000, 5000)
            fake_data = fake_data.append(pd.DataFrame(
                {'item_type_id': item_type_id, 'item_name': item_name,
                 'cost_amt': cost_amt}, index=[0]), ignore_index=True)

        fake_data.to_csv('./data/' + 'product_view' + '.csv',
                         index=False, sep=',', header=True, encoding="utf_8_sig")

    def product_table(self):
        """
        产品维表
        10个产品
        :return:
        """
        # 数据数量
        data_size = 1000
        fake_data = pd.DataFrame(columns=('item_id', 'create_time', 'warehouse_id',
                                          'exist_state', 'color_id', 'item_type_id'))
        for i in range(data_size):
            item_id = f"{i:015}"
            create_time = self.fake.date_between(start_date="-9y")
            warehouse_id = f"{self.fake.pyint(0, 9):05}"
            exist_state = self.fake.pyint(0, 1)
            item_type_id = f"{self.fake.pyint(0, 9):04}"
            buy_amt = self.fake.pyint(5000, 10000)
            fake_data = fake_data.append(pd.DataFrame(
                {'item_id': item_id, 'create_time': create_time, 'warehouse_id': warehouse_id,
                 'exist_state': exist_state,
                 'item_type_id': item_type_id}, index=[0]), ignore_index=True)

        fake_data.to_csv('./data/' + 'product_table' + '.csv',
                         index=False, sep=',', header=True, encoding="utf_8_sig")

    def city_table(self):
        city_name = ['北京', '上海', '广州', '深圳', '合肥', '成都', '武汉', '石家庄', '海口', '太原', '福建']
        city_id = ['00000', '00001', '00002', '00003', '00004', '00005', '00006', '00007', '00008', '00009', '00010']
        list_of_tuples = list(zip(city_id, city_name))
        fake_data = pd.DataFrame(list_of_tuples, columns=['city_id', 'city_name'])
        fake_data.to_csv('./data/' + 'city_table' + '.csv',
                         index=False, sep=',', header=True, encoding="utf_8_sig")

    def upload_S3(self, filepath):
        try:
            self.s3_client.create_bucket(Bucket="您的bucket名", ACL = 'public-read')
        finally:
            print("")


if __name__ == '__main__':
    faker = DataFaker()
    faker.city_table()
    # faker.sales_table()
    # faker.product_view()
    # faker.product_table()
