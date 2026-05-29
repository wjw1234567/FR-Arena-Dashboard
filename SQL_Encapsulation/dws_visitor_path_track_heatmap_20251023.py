import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime
import warnings
from Logger import Logger
from joblib import Parallel, delayed
warnings.filterwarnings("ignore")  # 忽略pandas无关警告


class TrackHeatmap_1030:

    def __init__(self, host=['localhost','localhost'], port=[9000,9000], user=['default','default'], password=['',''], database=['default','default'],prefix=None,date_list=[]):

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.client = Client(host=host[0], port=port[0], user=user[0], password=password[0], database=database[0])
        self.wd_client = Client(host=host[1], port=port[1], user=user[1], password=password[1], database=database[1])


        self.prefix=prefix
        self.logger = Logger(log_dir="./logs", prefix=self.prefix)
        self.date_list=date_list
        self.target_table=prefix



    def read_raw_data_from_ck(self,client, date):
        """从ClickHouse读取原始数据（仅读取需要的字段，减少数据量）"""
        # 提取z0_time中的日期，用于过滤原始数据（减少拉取数据量）

        query = f"""
            SELECT 
                profile_id,
                capture_time,
                member_tier,
                age,
                gender,
                region_name
            FROM dwd_user_capture_detail
            WHERE toDate(capture_time) = %(date)s  order by profile_id,capture_time
        """
        print(f"📥 正在读取原始数据（日期：{date}）...")
        try:
            # 执行查询并转换为DataFrame
            rows, cols = client.execute(query, with_column_types=True,params={"date":date})
            columns = [col[0] for col in cols]
            raw_df = pd.DataFrame(rows, columns=columns)
            # 转换capture_time为datetime类型（关键：后续计算时间差）
            raw_df["capture_time"] = pd.to_datetime(raw_df["capture_time"])
            print(f"✅ 原始数据读取完成（共{len(raw_df)}条记录，{raw_df['profile_id'].nunique()}个人员）")
            return raw_df
        except Exception as e:
            print(f"❌ 原始数据读取失败：{str(e)}")
            raise


    def write_result_to_ck(self,client, result_df):
        """将最终结果写入ClickHouse（批量插入，支持空数据跳过）"""
        if result_df.empty:
            print("⚠️  结果数据为空，跳过写入")
            return
        # 补充写入时间字段（与原SQL逻辑对齐）
        result_df["batch_time"] = datetime.now()

        # 构建插入SQL（处理字段名转义，避免特殊字符报错）
        columns = list(result_df.columns)
        escaped_cols = [f"`{col}`" for col in columns]  # 字段名加`转义
        insert_sql = f"INSERT INTO {self.target_table} ({','.join(escaped_cols)}) VALUES"

        # 转换数据格式（ClickHouse要求tuple列表）
        data = [tuple(row) for row in result_df.itertuples(index=False, name=None)]

        try:
            client.execute(insert_sql, data)
            print(f"✅ 结果写入完成（表：{self.target_table}，共{len(result_df)}条记录）")
        except Exception as e:
            print(f"❌ 结果写入失败：{str(e)}")
            raise


    # ==========================
    # 核心逻辑：DataFrame实现SQL功能
    # ==========================
    def preprocess_raw_data(self,raw_df):
        """预处理原始数据：衍生SQL中的date、date_hour、z0_time、age_range字段"""
        print("🔧 正在预处理原始数据...")
        prep_df = raw_df.assign(
            # 衍生日期字段（对应SQL的toDate(capture_time)）
            date=raw_df["capture_time"].dt.date,
            # 衍生小时字段（对应SQL的formatDateTime('%H:00')）
            date_hour=raw_df["capture_time"].dt.strftime("%H:00"),
            # 衍生z0_time字段（对应SQL的formatDateTime('%Y-%m-%d %H:00')）
            # z0_time=raw_df["capture_time"].dt.strftime("%Y-%m-%d %H:00"),
            z0_time=raw_df["capture_time"].dt.floor("H"),
            # 衍生年龄区间（对应SQL的age_range case when）


            age_range=np.select(
                [
                    (raw_df["age"] >= 0) & (raw_df["age"] <= 20),
                    (raw_df["age"] >= 21) & (raw_df["age"] <= 39),
                    (raw_df["age"] >= 40) & (raw_df["age"] <= 65),
                    (raw_df["age"] > 65)
                ],
                ["0-20", "21-39", "40-65", "65+"],
                default=""
            )
        ).dropna(subset=["age_range"])  # 过滤无效年龄区间
        print(f"✅ 数据预处理完成（有效记录数：{len(prep_df)}）")
        return prep_df



    @staticmethod
    def process_single_profile(profile_id, prep_df):
        """
        单个 profile_id 的完整处理逻辑（与原逻辑一致，适配 Parallel 调用）
        """


        try:
            # 提取当前 profile_id 的数据（独立数据，无线程安全问题）
            df = prep_df[prep_df["profile_id"] == profile_id].copy()
            if len(df) < 2:  # 少于2条记录无法自我关联，返回空
                return pd.DataFrame()

            # 自我关联：给 v2 表非关联键加后缀，避免字段冲突
            v2_df = df.add_suffix("_v2")

            v2_df = v2_df.rename(columns={
                "profile_id_v2": "profile_id",
                "date_v2": "date"
            })


            # 执行关联（关联键同名，无报错）
            merged_df = pd.merge(
                left=df,
                right=v2_df,
                on=["profile_id", "date"],
                how="inner"
            )

            # 过滤时间差（≤60分钟，排除自身）
            merged_df["diff_min"] = (merged_df["capture_time_v2"] - merged_df["capture_time"]).dt.total_seconds() / 60

            merged_df = merged_df[
                (merged_df["diff_min"].abs() <= 60)
                # (merged_df["capture_time"] != merged_df["capture_time_v2"])
                ]
            if merged_df.empty:
                return pd.DataFrame()

            # 计算 off_bin 分桶
            conditions = [
                (merged_df["diff_min"] == 0),
                (merged_df["diff_min"] > 0) & (merged_df["diff_min"] <= 15),
                (merged_df["diff_min"] >= -15) & (merged_df["diff_min"] < 0),
                (merged_df["diff_min"] >= 16) & (merged_df["diff_min"] <= 60),
                (merged_df["diff_min"] >= -60) & (merged_df["diff_min"] <= -16)
            ]
            values = [
                0,
                (merged_df["diff_min"] // 5 + 1) * 5,
                (merged_df["diff_min"] // 5 ) * 5,
                (merged_df["diff_min"] // 15 + 1) * 15,
                (merged_df["diff_min"] // 15 ) * 15
            ]
            merged_df["off_bin"] = np.select(conditions, values, default=None)

            target_bins = [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60]

            # 过滤目标 off_bin
            merged_df = merged_df[
                merged_df["off_bin"].notna() &
                merged_df["off_bin"].isin(target_bins)
                ]

            merged_df["off_bin"]=merged_df['off_bin'].astype(int)


            if merged_df.empty:
                return pd.DataFrame()

            # 分组聚合：找每个分组下时间差最小的记录（对应 SQL 的 argMin）
            merged_df["abs_diff_min"] = merged_df["diff_min"].abs()
            group_keys = ["date", "date_hour", "profile_id", "region_name", "z0_time", "off_bin", "member_tier",
                          "age_range", "gender"]
            min_diff_idx = merged_df.groupby(group_keys)["abs_diff_min"].idxmin()

            result = merged_df.loc[min_diff_idx].assign(
                area_at_off=merged_df.loc[min_diff_idx, "region_name_v2"]
            )

            # 保留最终字段（与 SQL 结果对齐）
            final_fields = ["date", "date_hour", "profile_id", "region_name", "z0_time", "off_bin", "area_at_off",
                            "member_tier", "age_range", "gender"]



            return result[final_fields].reset_index(drop=True)

        except Exception as e:
            print(f"❌ 处理 profile_id={profile_id} 失败：{str(e)}")
            return pd.DataFrame()



    # ==========================
    # 3. 核心：Parallel 多线程调度
    # ==========================
    def process_with_parallel(self,prep_df, n_jobs=-1, verbose=10):
        """
        用 joblib.Parallel 并行处理所有 profile_id
        :param prep_df: 预处理后的全量数据
        :param filter_cond: 过滤条件（target_bins）
        :param n_jobs: 并行数（-1 表示使用所有 CPU 核心，建议默认）
        :param verbose: 日志详细程度（10 表示每完成10个任务打印一次）
        :return: 合并后的最终结果
        """
        # 1. 获取所有唯一的 profile_id（确保无遗漏）
        profile_ids = prep_df["profile_id"].unique()
        total_profiles = len(profile_ids)
        print(f"📊 开始 Parallel 并行处理（共 {total_profiles} 个 profile_id，并行数：{n_jobs if n_jobs != -1 else '全部CPU核心'}）")

        # 2. 并行执行：用 delayed 包装单任务，Parallel 调度
        # delayed(process_single_profile) 会自动将每个 profile_id 作为独立任务
        results = Parallel(
            n_jobs=-1,  # 并行数（-1 自动适配CPU核心）
            verbose=verbose,  # 日志频率
            backend="threading"  # 用多线程（避免多进程的内存拷贝，更高效）
        )(
            delayed(self.process_single_profile)(pid, prep_df)
            for pid in profile_ids  # 遍历所有 profile_id，生成任务列表
        )

        # 3. 合并结果：过滤空 DataFrame，避免 concat 报错
        valid_results = [df for df in results if not df.empty]
        if not valid_results:
            print("⚠️ 所有 profile_id 均无有效结果，返回空数据")
            return pd.DataFrame()  # final_fields 与单任务返回字段一致

        final_df = pd.concat(valid_results, ignore_index=True)
        print(f"✅ Parallel 处理完成（总有效记录数：{len(final_df)}，覆盖 {final_df['profile_id'].nunique()} 个 profile_id）")
        return final_df




    # ==========================
    # 主函数：全流程调度
    # ==========================
    def main(self):
        print("=" * 50)
        print("📅 开始执行：ClickHouse SQL转DataFrame处理流程")
        print("=" * 50)

        # 1. 初始化ClickHouse客户端
        client=self.client

        try:

            for date in self.date_list:

                # 删除target表的数据
                delete_sql = f"alter table {target_table} delete where date = %(date)s"
                self.wd_client.execute(delete_sql, params={"date": date})

            # 2. 读取原始数据
                raw_df = self.read_raw_data_from_ck(client, date)
                if raw_df.empty:
                    print("⚠️  无原始数据，程序终止")
                    return

                # 3. 预处理原始数据
                prep_df = self.preprocess_raw_data(raw_df)

                # 4. 分块处理核心逻辑
                final_df = self.process_with_parallel(prep_df=prep_df,  n_jobs=-1, verbose=10)


                # print(final_df)

                # 5. 结果写入ClickHouse
                self.write_result_to_ck(client, final_df)

                print("=" * 50)
                print("🎉 全流程执行完成！")
                print("=" * 50)
        except Exception as e:
            print(f"=" * 50)
            print(f"❌ 程序执行失败：{str(e)}")
            print("=" * 50)
        finally:
            # 关闭ClickHouse连接
            client.disconnect()
            print("🔌 ClickHouse连接已关闭")


# ==========================
# 程序入口：直接运行
# ==========================
if __name__ == "__main__":


    target_table = "dws_visitor_path_track_heatmap_1024"
    date_list = pd.date_range("2025-08-25", "2025-08-27").strftime("%Y-%m-%d").tolist()
    trackheatmap = TrackHeatmap_1030(host=["localhost", "localhost"], port=[9000, 9000], user=["default", "default"],
                                password=["ck_test", "ck_test"], database=["Facial", "Facial"], prefix=target_table,date_list=date_list)

    trackheatmap.main()