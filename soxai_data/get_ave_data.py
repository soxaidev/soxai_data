from soxai_data import DataLoader
import pandas as pd
import datetime
import schedule


class CsvFile:
    
    def __init__(self):
        self.has_output_file_cnt = False

    def read_csv_df(self, input_file):
        df = pd.read_csv(input_file)
        return df
    
    def write_df_csv(self, df, output_file):
        df.to_csv(output_file, index = False)

    def write_csv_sort_index(self, df, input_file):
        df_sorted = df.sort_values(['uid', 'start_date']).reset_index(drop=True)
        df_sorted.to_csv(input_file, mode='w')



class InfluxDb:

    def __init__(self, api_key):
        self.api_key = api_key
        self.sx_data = self.initialize_dataloder()

    def initialize_dataloder(self):
        sx_data = DataLoader(token=self.api_key)
        return sx_data

    def get_daily_data_by_uid(self, start_date=None, end_date=None, convert_to_local_time=False, uid_list=[], timeout=60.0):
        try:
            df = self.sx_data.getDailyData(start_date, end_date, convert_to_local_time, uid_list, timeout)
            return df
        except Exception as e:
            print(f'failed to get data from influxdb  {e}')
            return None

class DataProcessing:

    def __init__(self):
        pass

    def sort_df_by_time(self, df):
        # 日付順にソートし、日付をdatetime型に変換
        df_sorted = df.sort_values('_time')
        df_sorted['_time'] = pd.to_datetime(df_sorted['_time'])
        return df_sorted

    def get_average_data(self, df, start_end_date_dict):
        csvFile = CsvFile()
        # 取得したデータのstart_dateとend_dateとUIDを取得する
        df_start_date = pd.DataFrame().from_dict({"start_date": [start_end_date_dict['start_date']]}).reset_index(drop=True)
        df_end_date = pd.DataFrame().from_dict({"end_date": [start_end_date_dict['end_date']]}).reset_index(drop=True)
        df_uid = df.head(1)['uid'].reset_index(drop=True)
        # 不要なデータを削除する
        df_fields = df.drop(['_start', '_stop', '_measurement', '_time', 'month', 'uid', 'workday', 'year', 'year_week'], axis=1)
        # 空データを0に変換
        for column in df_fields.select_dtypes(include="object").columns:
            df_fields[column] = pd.to_numeric(df_fields[column], errors='coerce')
        # 取得したデータの各フィールドの平均値を取得する
        df_average = pd.DataFrame(df_fields.mean()).transpose().reset_index(drop=True)
        # UID, start_date, end_date, フィールド を結合する
        df_result = pd.concat([df_uid, df_start_date, df_end_date, df_average], axis=1)
        return df_result

    # 日付計算（開始日も1日目に含む）
    def get_date_after_including_the_date(self, start_date, plus_cnt):
        end_date = start_date + datetime.timedelta(days=plus_cnt - 1)
        return end_date

    # データ取得開始日から30日(default)毎の日付期間を取得する
    def make_list_period_date(self, df, current_date, period_cnt=30):
        # 最初の日を取得
        first_date = df['_time'].iloc[0]
        period_date_list = []
        while True:
            end_date = self.get_date_after_including_the_date(first_date, period_cnt)
            date_period = {
                'start_date': first_date,
                'end_date': end_date,
            }
            period_date_list.append(date_period)
            if end_date < current_date:
                first_date = self.get_date_after_including_the_date(end_date, 2)
            else:
                break
        return period_date_list

    # 日付期間内のデータをDataFrameから取得し、リストにして返却する
    def get_period_date_df(self, df, date_period_list):
        df_period_list = []
        for date_period in date_period_list:
            # dfから日付期間に該当するデータを抽出
            df_period = df[(df['_time'] >= date_period['start_date']) & (df['_time'] <= date_period['end_date'])]
            df_period_list.append(df_period)
        return df_period_list

    def get_average_datas(self, df, current_date, period_cnt):
        # 返却値
        df_result = pd.DataFrame()
        # _timeで日付順にする
        df_sorted = self.sort_df_by_time(df)
        # 1番最初の日付から今日まで、30日毎の開始日と終了日を格納したリストを作成する
        date_period_list = self.make_list_period_date(df_sorted, current_date, period_cnt)
        # 取得した期間のデータを取得する
        df_period_list = self.get_period_date_df(df_sorted, date_period_list)
        # ループ開始
        for list_cnt in range(len(date_period_list)):
            # 指定数のデータを取得する
            df_group = df_period_list[list_cnt]
            # 取得したデータが0件の場合、ループ終了
            if len(df_group) < 1:
                break
            # 指定数のデータの平均値を取得する
            df_processed = self.get_average_data(df_group, date_period_list[list_cnt])
            # 結合した結果をresult dataframeに追加していく
            df_result = pd.concat([df_result, df_processed]).reset_index(drop=True)
        return df_result

class AverageDataExecutor:

    def __init__(self, api_key: str, period_cnt: int, input_file: str, output_file_path: str):
        self.api_key = api_key
        self.period_cnt = period_cnt
        self.input_file = input_file
        self.output_file_path = output_file_path
        self.task_executed = False

    def within_time_range(self, start_time, end_time):
        if start_time is None:
            return True
        if end_time is None:
            return True
        current_time = datetime.datetime.now().time()
        return start_time <= current_time <= end_time

    def get_time(self, time_str):
        # string -> datetime.time
        if time_str is None:
            return None
        try:
            time_list = time_str.split(':')
            datetime_time = datetime.time(int(time_list[0]), int(time_list[1]), 0)
            return datetime_time
        except Exception as e:
            print('input time is not correct : ', e)

    def execute(self, process_start_time=None, process_end_time=None):

        # プログラム実行時の引数（処理開始時間、終了時間）を取得する
        start_time = self.get_time(process_start_time)
        end_time = self.get_time(process_end_time)

        # 指定時間内でない場合、処理を終了する
        if not self.within_time_range(start_time, end_time):
            print('not within_time_range')
            return

        # 開始時間測定
        start_datetime = datetime.datetime.now(datetime.timezone.utc)

        # 定数
        input_file = self.input_file
        output_file_path = f'{self.output_file_path}/{start_datetime.strftime("%Y%m%d%H%M%S")}_user_uid.csv'
        not_processed_uid_path = f'{self.output_file_path}/{start_datetime.strftime("%Y%m%d%H%M%S")}_not_processed_uid.csv'
        failed_processed_uid_path = f'{self.output_file_path}/{start_datetime.strftime("%Y%m%d%H%M%S")}_failed_uid.csv'
        # 取得開始日指定
        start_date = datetime.datetime(year=2022, month=3, day=1, hour=0, minute=0, second=0)

        # 変数
        # 処理済みのDataFrameを保存
        df_result_list = pd.DataFrame()
        # 失敗したUIDを保存
        failed_processed_uid_list = []

        # インスタンス作成
        csvFile = CsvFile()
        influxDb = InfluxDb(self.api_key)
        dataProcessing = DataProcessing()

        # CSVから入力されたUIDを取得
        df_uid_list = csvFile.read_csv_df(input_file)

        try:
            for uid in df_uid_list['UID list']:
                print(f'processing uid {uid}')
                if not self.within_time_range(start_time, end_time):
                    # 指定時間内でなければ処理を終了する
                    self.task_executed = True
                    print('ended tasks for today')
                    break
                else:
                    # InfluxDBからデータを取得
                    df = influxDb.get_daily_data_by_uid(start_date, None, False, [uid], 60.0)
                    if df is None:
                        # 取得に失敗した場合、取得失敗リストに追加
                        failed_processed_uid_list.append(uid)
                        print('failed_get_from_influxDB')
                        continue
                    if len(df) < 1:
                        continue
                    # データ加工
                    df_result = dataProcessing.get_average_datas(df, start_datetime, self.period_cnt)
                    # 結果リストに追加
                    df_result_list = pd.concat([df_result_list, df_result], axis=0)

        except Exception as e:
            print(f'Error in process data : {e}')

        if len(df_result_list) > 0:

            # 取得したデータをUID、開始時間にソートし、indexの振り直しを行いCSV出力を行う
            csvFile.write_csv_sort_index(df_result_list, output_file_path)

            # 未処理のUIDをnot_processed_uid.csvに出力する
            df_not_processed_uid = df_uid_list[~df_uid_list['UID list'].isin(df_result_list['uid'])]
            if len(df_not_processed_uid) > 0:
                csvFile.write_df_csv(df_not_processed_uid, not_processed_uid_path)
        else:
            df_not_processed_uid = df_uid_list
            csvFile.write_df_csv(df_uid_list, not_processed_uid_path)

        if len(failed_processed_uid_list) > 0:
            # 失敗したUIDをfailed_processed_uid_list.csvに出力する
            csvFile.write_df_csv(pd.DataFrame(failed_processed_uid_list, columns=['UID list']), failed_processed_uid_path)

        # 未処理のUIDが存在しない場合、処理終了フラグをTrueにする
        if len(df_not_processed_uid) == 0:
            self.task_executed = True

        # 開始時間測定
        end_datetime = datetime.datetime.now(datetime.timezone.utc)

        print(f'start_datetime : {start_datetime}')
        print(f'end_datetime   : {end_datetime}')
        print(f'gap_time : {end_datetime - start_datetime}')


    def execute_scheduler(self, schedule_start_time, schedule_end_time):
        # The format of schedule_start_time and schedule_end_time is "hh:mm"

        # 処理開始時間、処理終了時間を取得する
        print(f'this program will start at {schedule_start_time} and end at {schedule_end_time}')

        # スケジューラにタスクをセット
        schedule.every().day.at(schedule_start_time).do(self.execute, process_start_time=schedule_start_time, process_end_time=schedule_end_time)
        # タスク終了時にTrue
        self.task_executed = False

        while not self.task_executed:
            # 時間まで待機
            schedule.run_pending()

        print('All done')