from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, configure

# from myproject.datasets import utils
# @configure(profile=["EXECUTOR_MEMORY_MEDIUM", "DRIVER_MEMORY_EXTRA_LARGE"])

@transform_df(
    Output("ri.foundry.main.dataset.af773ed6-9223-41fa-a5cb-05f1db81be25"),
    project_wbs=Input("ri.foundry.main.dataset.d8dd1e17-bd52-4c88-a9ce-95398791f164"),
    civilpmis_calendar=Input("ri.foundry.main.dataset.c01f28c2-02f8-48b0-8329-a9688cb8fe6c"),
    project_task=Input("ri.foundry.main.dataset.014dbc54-c423-4e4f-aa19-ae6926203887"),
)
def compute(project_wbs, project_task, civilpmis_calendar):

    # 01 WBS에서 Union에 필요한 컬럼만 추출
    wbs = project_wbs.select(
        F.col("project_code").cast("string"),
        F.col("wbs_code").alias("id"),
        F.col("upper_wbs_code").cast("string").alias("upper_id"),
        F.col("wbs_short_name").alias("code"),
        F.col("wbs_name").alias("name"),
        F.col("project_node_flag").alias("yn_root"),
        F.lit("N").alias("yn_leaf"),
        F.lit("").cast("string").alias("target_begin_date"),
        F.lit("").cast("string").alias("target_end_date"),
        F.lit("").cast("string").alias("act_begin_date"),
        F.lit("").cast("string").alias("act_end_date"),
        F.lit(0).alias("total_float")
    )

    # wbs = (
    #     wbs
    #     .filter(F.col("project_code").isin("76212"))
    # )

    # 02 TASK에서 Union에 필요한 컬럼만 추출
    task = project_task.select(
        F.col("project_code").cast("string"),
        F.col("task_code_1").alias("id"),
        F.col("wbs_code").cast("string").alias("upper_id"),
        F.col("task_code").alias("code"),
        F.col("task_name").alias("name"),
        F.lit("N").alias("yn_root"),
        F.lit("Y").alias("yn_leaf"),
        F.col("target_begin_date").cast("date").alias("target_begin_date"),
        F.col("target_end_date").cast("date").alias("target_end_date"),
        F.col("act_begin_date").cast("date").alias("act_begin_date"),
        F.col("act_end_date").cast("date").alias("act_end_date"),
        F.col("total_float_hr_count").alias("total_float")
    )

    # task = (
    #     task
    #     .filter(F.col("task_type").isin("TT_Task", "TT_FinMile", "TT_Mile"))
    #     #.filter(F.col("code").contains("170373"))
    #     # .filter(F.col("project_code").isin("76212"))  #135737
    # )

    # 03 WBS와 TASK를 Union
    wbs_task = wbs.unionAll(task)

    # 04 id와 upper_id JOIN을 위한 WBS/TASK 복제 (레벨 개수 만큼)
    wbs_task_lv1 = wbs_task.select(
        F.col("project_code").alias("lv1_project_code"),
        F.col("id").alias("lv1_id"),
        F.col("upper_id").alias("lv1_upper_id"),
        F.col("code").alias("lv1_code"),
        F.col("name").alias("lv1_name"),
        F.col("yn_root").alias("lv1_yn_root"),
        F.col("yn_leaf").alias("lv1_yn_leaf"),
        F.col("target_begin_date").alias("lv1_target_begin_date"),
        F.col("target_end_date").alias("lv1_target_end_date"),
        F.col("act_begin_date").alias("lv1_act_begin_date"),
        F.col("act_end_date").alias("lv1_act_end_date"),
        F.col("total_float").alias("lv1_total_float")
        ).filter(F.col("yn_root") == 'Y')

    wbs_task_lv2 = wbs_task.select(
        F.col("project_code").alias("lv2_project_code"),
        F.col("id").alias("lv2_id"),
        F.col("upper_id").alias("lv2_upper_id"),
        F.col("code").alias("lv2_code"),
        F.col("name").alias("lv2_name"),
        F.col("yn_root").alias("lv2_yn_root"),
        F.col("yn_leaf").alias("lv2_yn_leaf"),
        F.col("target_begin_date").alias("lv2_target_begin_date"),
        F.col("target_end_date").alias("lv2_target_end_date"),
        F.col("act_begin_date").alias("lv2_act_begin_date"),
        F.col("act_end_date").alias("lv2_act_end_date"),
        F.col("total_float").alias("lv2_total_float")
        )

    wbs_task_lv3 = wbs_task.select(
        F.col("project_code").alias("lv3_project_code"),
        F.col("id").alias("lv3_id"),
        F.col("upper_id").alias("lv3_upper_id"),
        F.col("code").alias("lv3_code"),
        F.col("name").alias("lv3_name"),
        F.col("yn_root").alias("lv3_yn_root"),
        F.col("yn_leaf").alias("lv3_yn_leaf"),
        F.col("target_begin_date").alias("lv3_target_begin_date"),
        F.col("target_end_date").alias("lv3_target_end_date"),
        F.col("act_begin_date").alias("lv3_act_begin_date"),
        F.col("act_end_date").alias("lv3_act_end_date"),
        F.col("total_float").alias("lv3_total_float")
        )

    wbs_task_lv4 = wbs_task.select(
        F.col("project_code").alias("lv4_project_code"),
        F.col("id").alias("lv4_id"),
        F.col("upper_id").alias("lv4_upper_id"),
        F.col("code").alias("lv4_code"),
        F.col("name").alias("lv4_name"),
        F.col("yn_root").alias("lv4_yn_root"),
        F.col("yn_leaf").alias("lv4_yn_leaf"),
        F.col("target_begin_date").alias("lv4_target_begin_date"),
        F.col("target_end_date").alias("lv4_target_end_date"),
        F.col("act_begin_date").alias("lv4_act_begin_date"),
        F.col("act_end_date").alias("lv4_act_end_date"),
        F.col("total_float").alias("lv4_total_float")
        )

    wbs_task_lv5 = wbs_task.select(
        F.col("project_code").alias("lv5_project_code"),
        F.col("id").alias("lv5_id"),
        F.col("upper_id").alias("lv5_upper_id"),
        F.col("code").alias("lv5_code"),
        F.col("name").alias("lv5_name"),
        F.col("yn_root").alias("lv5_yn_root"),
        F.col("yn_leaf").alias("lv5_yn_leaf"),
        F.col("target_begin_date").alias("lv5_target_begin_date"),
        F.col("target_end_date").alias("lv5_target_end_date"),
        F.col("act_begin_date").alias("lv5_act_begin_date"),
        F.col("act_end_date").alias("lv5_act_end_date"),
        F.col("total_float").alias("lv5_total_float")
        )

    wbs_task_lv6 = wbs_task.select(
        F.col("project_code").alias("lv6_project_code"),
        F.col("id").alias("lv6_id"),
        F.col("upper_id").alias("lv6_upper_id"),
        F.col("code").alias("lv6_code"),
        F.col("name").alias("lv6_name"),
        F.col("yn_root").alias("lv6_yn_root"),
        F.col("yn_leaf").alias("lv6_yn_leaf"),
        F.col("target_begin_date").alias("lv6_target_begin_date"),
        F.col("target_end_date").alias("lv6_target_end_date"),
        F.col("act_begin_date").alias("lv6_act_begin_date"),
        F.col("act_end_date").alias("lv6_act_end_date"),
        F.col("total_float").alias("lv6_total_float")
        )

    wbs_task_lv7 = wbs_task.select(
        F.col("project_code").alias("lv7_project_code"),
        F.col("id").alias("lv7_id"),
        F.col("upper_id").alias("lv7_upper_id"),
        F.col("code").alias("lv7_code"),
        F.col("name").alias("lv7_name"),
        F.col("yn_root").alias("lv7_yn_root"),
        F.col("yn_leaf").alias("lv7_yn_leaf"),
        F.col("target_begin_date").alias("lv7_target_begin_date"),
        F.col("target_end_date").alias("lv7_target_end_date"),
        F.col("act_begin_date").alias("lv7_act_begin_date"),
        F.col("act_end_date").alias("lv7_act_end_date"),
        F.col("total_float").alias("lv7_total_float")
        )

    wbs_task_lv8 = wbs_task.select(
        F.col("project_code").alias("lv8_project_code"),
        F.col("id").alias("lv8_id"),
        F.col("upper_id").alias("lv8_upper_id"),
        F.col("code").alias("lv8_code"),
        F.col("name").alias("lv8_name"),
        F.col("yn_root").alias("lv8_yn_root"),
        F.col("yn_leaf").alias("lv8_yn_leaf"),
        F.col("target_begin_date").alias("lv8_target_begin_date"),
        F.col("target_end_date").alias("lv8_target_end_date"),
        F.col("act_begin_date").alias("lv8_act_begin_date"),
        F.col("act_end_date").alias("lv8_act_end_date"),
        F.col("total_float").alias("lv8_total_float")
        )

    wbs_task_lv9 = wbs_task.select(
        F.col("project_code").alias("lv9_project_code"),
        F.col("id").alias("lv9_id"),
        F.col("upper_id").alias("lv9_upper_id"),
        F.col("code").alias("lv9_code"),
        F.col("name").alias("lv9_name"),
        F.col("yn_root").alias("lv9_yn_root"),
        F.col("yn_leaf").alias("lv9_yn_leaf"),
        F.col("target_begin_date").alias("lv9_target_begin_date"),
        F.col("target_end_date").alias("lv9_target_end_date"),
        F.col("act_begin_date").alias("lv9_act_begin_date"),
        F.col("act_end_date").alias("lv9_act_end_date"),
        F.col("total_float").alias("lv9_total_float")
        )

    # 06 UNION을 위한 Level별 컬럼 정리
    wbs_task_lv1_df = (
        wbs_task_lv1
        .select(
            F.col("lv1_project_code").alias("project_code"),
            F.lit("1").alias("lv"),
            F.col("lv1_code").alias("code"),
            F.col("lv1_name").alias("name"),
            F.col("lv1_id").alias("id"),
            F.col("lv1_upper_id").alias("upper_id"),
            F.col("lv1_yn_leaf").alias("yn_leaf"),
            F.col("lv1_target_begin_date").alias("target_begin_date"),
            F.col("lv1_target_end_date").alias("target_end_date"),
            F.col("lv1_act_begin_date").alias("act_begin_date"),
            F.col("lv1_act_end_date").alias("act_end_date"),
            F.col("lv1_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.lit("").alias("lv2_id"),
            F.lit("").alias("lv2_name"),
            F.lit("").alias("lv3_id"),
            F.lit("").alias("lv3_name"),
            F.lit("").alias("lv4_id"),
            F.lit("").alias("lv4_name"),
            F.lit("").alias("lv5_id"),
            F.lit("").alias("lv5_name"),
            F.lit("").alias("lv6_id"),
            F.lit("").alias("lv6_name"),
            F.lit("").alias("lv7_id"),
            F.lit("").alias("lv7_name"),
            F.lit("").alias("lv8_id"),
            F.lit("").alias("lv8_name"),
            F.lit("").alias("lv9_id"),
            F.lit("").alias("lv9_name")
        )
        # .filter(F.col("id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv1_df = wbs_task_lv1_df.distinct()

    wbs_task_lv2_df = (
        wbs_task_lv1
        .join(wbs_task_lv2, on=[wbs_task_lv1.lv1_project_code == wbs_task_lv2.lv2_project_code, wbs_task_lv1.lv1_id == wbs_task_lv2.lv2_upper_id], how='inner')
        .select(
            F.col("lv2_project_code").alias("project_code"),
            F.lit("2").alias("lv"),
            F.concat("lv1_code", F.lit("."), "lv2_code").alias("code"),
            F.col("lv2_name").alias("name"),
            F.col("lv2_id").alias("id"),
            F.col("lv2_upper_id").alias("upper_id"),
            F.col("lv2_yn_leaf").alias("yn_leaf"),
            F.col("lv2_target_begin_date").alias("target_begin_date"),
            F.col("lv2_target_end_date").alias("target_end_date"),
            F.col("lv2_act_begin_date").alias("act_begin_date"),
            F.col("lv2_act_end_date").alias("act_end_date"),
            F.col("lv2_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.col("lv2_id").alias("lv2_id"),
            F.col("lv2_name").alias("lv2_name"),
            F.lit("").alias("lv3_id"),
            F.lit("").alias("lv3_name"),
            F.lit("").alias("lv4_id"),
            F.lit("").alias("lv4_name"),
            F.lit("").alias("lv5_id"),
            F.lit("").alias("lv5_name"),
            F.lit("").alias("lv6_id"),
            F.lit("").alias("lv6_name"),
            F.lit("").alias("lv7_id"),
            F.lit("").alias("lv7_name"),
            F.lit("").alias("lv8_id"),
            F.lit("").alias("lv8_name"),
            F.lit("").alias("lv9_id"),
            F.lit("").alias("lv9_name")
        )
        # .filter(F.col("lv2_id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv2_df = wbs_task_lv2_df.distinct()

    wbs_task_lv3_df = (
        wbs_task_lv1
        .join(wbs_task_lv2, on=[wbs_task_lv1.lv1_project_code == wbs_task_lv2.lv2_project_code, wbs_task_lv1.lv1_id == wbs_task_lv2.lv2_upper_id], how='left')
        .join(wbs_task_lv3, on=[wbs_task_lv2.lv2_project_code == wbs_task_lv3.lv3_project_code, wbs_task_lv2.lv2_id == wbs_task_lv3.lv3_upper_id], how='inner')
        .select(
            F.col("lv3_project_code").alias("project_code"),
            F.lit("3").alias("lv"),
            F.concat("lv1_code", F.lit("."), "lv2_code", F.lit("."), "lv3_code").alias("code"),
            F.col("lv3_name").alias("name"),
            F.col("lv3_id").alias("id"),
            F.col("lv3_upper_id").alias("upper_id"),
            F.col("lv3_yn_leaf").alias("yn_leaf"),
            F.col("lv3_target_begin_date").alias("target_begin_date"),
            F.col("lv3_target_end_date").alias("target_end_date"),
            F.col("lv3_act_begin_date").alias("act_begin_date"),
            F.col("lv3_act_end_date").alias("act_end_date"),
            F.col("lv3_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.col("lv2_id").alias("lv2_id"),
            F.col("lv2_name").alias("lv2_name"),
            F.col("lv3_id").alias("lv3_id"),
            F.col("lv3_name").alias("lv3_name"),
            F.lit("").alias("lv4_id"),
            F.lit("").alias("lv4_name"),
            F.lit("").alias("lv5_id"),
            F.lit("").alias("lv5_name"),
            F.lit("").alias("lv6_id"),
            F.lit("").alias("lv6_name"),
            F.lit("").alias("lv7_id"),
            F.lit("").alias("lv7_name"),
            F.lit("").alias("lv8_id"),
            F.lit("").alias("lv8_name"),
            F.lit("").alias("lv9_id"),
            F.lit("").alias("lv9_name")
        )
        # .filter(F.col("lv3_id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv3_df = wbs_task_lv3_df.distinct()

    wbs_task_lv4_df = (
        wbs_task_lv1
        .join(wbs_task_lv2, on=[wbs_task_lv1.lv1_project_code == wbs_task_lv2.lv2_project_code, wbs_task_lv1.lv1_id == wbs_task_lv2.lv2_upper_id], how='left')
        .join(wbs_task_lv3, on=[wbs_task_lv2.lv2_project_code == wbs_task_lv3.lv3_project_code, wbs_task_lv2.lv2_id == wbs_task_lv3.lv3_upper_id], how='left')
        .join(wbs_task_lv4, on=[wbs_task_lv3.lv3_project_code == wbs_task_lv4.lv4_project_code, wbs_task_lv3.lv3_id == wbs_task_lv4.lv4_upper_id], how='inner')
        .select(
            F.col("lv4_project_code").alias("project_code"),
            F.lit("4").alias("lv"),
            F.concat("lv1_code", F.lit("."), "lv2_code", F.lit("."), "lv3_code", F.lit("."), "lv4_code").alias("code"),
            F.col("lv4_name").alias("name"),
            F.col("lv4_id").alias("id"),
            F.col("lv4_upper_id").alias("upper_id"),
            F.col("lv4_yn_leaf").alias("yn_leaf"),
            F.col("lv4_target_begin_date").alias("target_begin_date"),
            F.col("lv4_target_end_date").alias("target_end_date"),
            F.col("lv4_act_begin_date").alias("act_begin_date"),
            F.col("lv4_act_end_date").alias("act_end_date"),
            F.col("lv4_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.col("lv2_id").alias("lv2_id"),
            F.col("lv2_name").alias("lv2_name"),
            F.col("lv3_id").alias("lv3_id"),
            F.col("lv3_name").alias("lv3_name"),
            F.col("lv4_id").alias("lv4_id"),
            F.col("lv4_name").alias("lv4_name"),
            F.lit("").alias("lv5_id"),
            F.lit("").alias("lv5_name"),
            F.lit("").alias("lv6_id"),
            F.lit("").alias("lv6_name"),
            F.lit("").alias("lv7_id"),
            F.lit("").alias("lv7_name"),
            F.lit("").alias("lv8_id"),
            F.lit("").alias("lv8_name"),
            F.lit("").alias("lv9_id"),
            F.lit("").alias("lv9_name")
        )
        # .filter(F.col("lv4_id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv4_df = wbs_task_lv4_df.distinct()

    wbs_task_lv5_df = (
        wbs_task_lv1
        .join(wbs_task_lv2, on=[wbs_task_lv1.lv1_project_code == wbs_task_lv2.lv2_project_code, wbs_task_lv1.lv1_id == wbs_task_lv2.lv2_upper_id], how='left')
        .join(wbs_task_lv3, on=[wbs_task_lv2.lv2_project_code == wbs_task_lv3.lv3_project_code, wbs_task_lv2.lv2_id == wbs_task_lv3.lv3_upper_id], how='left')
        .join(wbs_task_lv4, on=[wbs_task_lv3.lv3_project_code == wbs_task_lv4.lv4_project_code, wbs_task_lv3.lv3_id == wbs_task_lv4.lv4_upper_id], how='left')
        .join(wbs_task_lv5, on=[wbs_task_lv4.lv4_project_code == wbs_task_lv5.lv5_project_code, wbs_task_lv4.lv4_id == wbs_task_lv5.lv5_upper_id], how='inner')
        .select(
            F.col("lv5_project_code").alias("project_code"),
            F.lit("5").alias("lv"),
            F.concat("lv1_code", F.lit("."), "lv2_code", F.lit("."), "lv3_code", F.lit("."), "lv4_code", F.lit("."), "lv5_code").alias("code"),
            F.col("lv5_name").alias("name"),
            F.col("lv5_id").alias("id"),
            F.col("lv5_upper_id").alias("upper_id"),
            F.col("lv5_yn_leaf").alias("yn_leaf"),
            F.col("lv5_target_begin_date").alias("target_begin_date"),
            F.col("lv5_target_end_date").alias("target_end_date"),
            F.col("lv5_act_begin_date").alias("act_begin_date"),
            F.col("lv5_act_end_date").alias("act_end_date"),
            F.col("lv5_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.col("lv2_id").alias("lv2_id"),
            F.col("lv2_name").alias("lv2_name"),
            F.col("lv3_id").alias("lv3_id"),
            F.col("lv3_name").alias("lv3_name"),
            F.col("lv4_id").alias("lv4_id"),
            F.col("lv4_name").alias("lv4_name"),
            F.col("lv5_id").alias("lv5_id"),
            F.col("lv5_name").alias("lv5_name"),
            F.lit("").alias("lv6_id"),
            F.lit("").alias("lv6_name"),
            F.lit("").alias("lv7_id"),
            F.lit("").alias("lv7_name"),
            F.lit("").alias("lv8_id"),
            F.lit("").alias("lv8_name"),
            F.lit("").alias("lv9_id"),
            F.lit("").alias("lv9_name")
        )
        # .filter(F.col("lv5_id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv5_df = wbs_task_lv5_df.distinct()

    wbs_task_lv6_df = (
        wbs_task_lv1
        .join(wbs_task_lv2, on=[wbs_task_lv1.lv1_project_code == wbs_task_lv2.lv2_project_code, wbs_task_lv1.lv1_id == wbs_task_lv2.lv2_upper_id], how='left')
        .join(wbs_task_lv3, on=[wbs_task_lv2.lv2_project_code == wbs_task_lv3.lv3_project_code, wbs_task_lv2.lv2_id == wbs_task_lv3.lv3_upper_id], how='left')
        .join(wbs_task_lv4, on=[wbs_task_lv3.lv3_project_code == wbs_task_lv4.lv4_project_code, wbs_task_lv3.lv3_id == wbs_task_lv4.lv4_upper_id], how='left')
        .join(wbs_task_lv5, on=[wbs_task_lv4.lv4_project_code == wbs_task_lv5.lv5_project_code, wbs_task_lv4.lv4_id == wbs_task_lv5.lv5_upper_id], how='left')
        .join(wbs_task_lv6, on=[wbs_task_lv5.lv5_project_code == wbs_task_lv6.lv6_project_code, wbs_task_lv5.lv5_id == wbs_task_lv6.lv6_upper_id], how='inner')
        .select(
            F.col("lv6_project_code").alias("project_code"),
            F.lit("6").alias("lv"),
            F.concat("lv1_code", F.lit("."), "lv2_code", F.lit("."), "lv3_code", F.lit("."), "lv4_code", F.lit("."), "lv5_code", F.lit("."), "lv6_code").alias("code"),
            F.col("lv6_name").alias("name"),
            F.col("lv6_id").alias("id"),
            F.col("lv6_upper_id").alias("upper_id"),
            F.col("lv6_yn_leaf").alias("yn_leaf"),
            F.col("lv6_target_begin_date").alias("target_begin_date"),
            F.col("lv6_target_end_date").alias("target_end_date"),
            F.col("lv6_act_begin_date").alias("act_begin_date"),
            F.col("lv6_act_end_date").alias("act_end_date"),
            F.col("lv6_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.col("lv2_id").alias("lv2_id"),
            F.col("lv2_name").alias("lv2_name"),
            F.col("lv3_id").alias("lv3_id"),
            F.col("lv3_name").alias("lv3_name"),
            F.col("lv4_id").alias("lv4_id"),
            F.col("lv4_name").alias("lv4_name"),
            F.col("lv5_id").alias("lv5_id"),
            F.col("lv5_name").alias("lv5_name"),
            F.col("lv6_id").alias("lv6_id"),
            F.col("lv6_name").alias("lv6_name"),
            F.lit("").alias("lv7_id"),
            F.lit("").alias("lv7_name"),
            F.lit("").alias("lv8_id"),
            F.lit("").alias("lv8_name"),
            F.lit("").alias("lv9_id"),
            F.lit("").alias("lv9_name")
        )
        # .filter(F.col("lv6_id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv6_df = wbs_task_lv6_df.distinct()

    wbs_task_lv7_df = (
        wbs_task_lv1
        .join(wbs_task_lv2, on=[wbs_task_lv1.lv1_project_code == wbs_task_lv2.lv2_project_code, wbs_task_lv1.lv1_id == wbs_task_lv2.lv2_upper_id], how='left')
        .join(wbs_task_lv3, on=[wbs_task_lv2.lv2_project_code == wbs_task_lv3.lv3_project_code, wbs_task_lv2.lv2_id == wbs_task_lv3.lv3_upper_id], how='left')
        .join(wbs_task_lv4, on=[wbs_task_lv3.lv3_project_code == wbs_task_lv4.lv4_project_code, wbs_task_lv3.lv3_id == wbs_task_lv4.lv4_upper_id], how='left')
        .join(wbs_task_lv5, on=[wbs_task_lv4.lv4_project_code == wbs_task_lv5.lv5_project_code, wbs_task_lv4.lv4_id == wbs_task_lv5.lv5_upper_id], how='left')
        .join(wbs_task_lv6, on=[wbs_task_lv5.lv5_project_code == wbs_task_lv6.lv6_project_code, wbs_task_lv5.lv5_id == wbs_task_lv6.lv6_upper_id], how='left')
        .join(wbs_task_lv7, on=[wbs_task_lv6.lv6_project_code == wbs_task_lv7.lv7_project_code, wbs_task_lv6.lv6_id == wbs_task_lv7.lv7_upper_id], how='inner')
        .select(
            F.col("lv7_project_code").alias("project_code"),
            F.lit("7").alias("lv"),
            F.concat("lv1_code", F.lit("."), "lv2_code", F.lit("."), "lv3_code", F.lit("."), "lv4_code", F.lit("."), "lv5_code", F.lit("."), "lv6_code", F.lit("."), "lv7_code").alias("code"),
            F.col("lv7_name").alias("name"),
            F.col("lv7_id").alias("id"),
            F.col("lv7_upper_id").alias("upper_id"),
            F.col("lv7_yn_leaf").alias("yn_leaf"),
            F.col("lv7_target_begin_date").alias("target_begin_date"),
            F.col("lv7_target_end_date").alias("target_end_date"),
            F.col("lv7_act_begin_date").alias("act_begin_date"),
            F.col("lv7_act_end_date").alias("act_end_date"),
            F.col("lv7_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.col("lv2_id").alias("lv2_id"),
            F.col("lv2_name").alias("lv2_name"),
            F.col("lv3_id").alias("lv3_id"),
            F.col("lv3_name").alias("lv3_name"),
            F.col("lv4_id").alias("lv4_id"),
            F.col("lv4_name").alias("lv4_name"),
            F.col("lv5_id").alias("lv5_id"),
            F.col("lv5_name").alias("lv5_name"),
            F.col("lv6_id").alias("lv6_id"),
            F.col("lv6_name").alias("lv6_name"),
            F.col("lv7_id").alias("lv7_id"),
            F.col("lv7_name").alias("lv7_name"),
            F.lit("").alias("lv8_id"),
            F.lit("").alias("lv8_name"),
            F.lit("").alias("lv9_id"),
            F.lit("").alias("lv9_name")
        )
        # .filter(F.col("lv7_id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv7_df = wbs_task_lv7_df.distinct()

    wbs_task_lv8_df = (
        wbs_task_lv1
        .join(wbs_task_lv2, on=[wbs_task_lv1.lv1_project_code == wbs_task_lv2.lv2_project_code, wbs_task_lv1.lv1_id == wbs_task_lv2.lv2_upper_id], how='left')
        .join(wbs_task_lv3, on=[wbs_task_lv2.lv2_project_code == wbs_task_lv3.lv3_project_code, wbs_task_lv2.lv2_id == wbs_task_lv3.lv3_upper_id], how='left')
        .join(wbs_task_lv4, on=[wbs_task_lv3.lv3_project_code == wbs_task_lv4.lv4_project_code, wbs_task_lv3.lv3_id == wbs_task_lv4.lv4_upper_id], how='left')
        .join(wbs_task_lv5, on=[wbs_task_lv4.lv4_project_code == wbs_task_lv5.lv5_project_code, wbs_task_lv4.lv4_id == wbs_task_lv5.lv5_upper_id], how='left')
        .join(wbs_task_lv6, on=[wbs_task_lv5.lv5_project_code == wbs_task_lv6.lv6_project_code, wbs_task_lv5.lv5_id == wbs_task_lv6.lv6_upper_id], how='left')
        .join(wbs_task_lv7, on=[wbs_task_lv6.lv6_project_code == wbs_task_lv7.lv7_project_code, wbs_task_lv6.lv6_id == wbs_task_lv7.lv7_upper_id], how='left')
        .join(wbs_task_lv8, on=[wbs_task_lv7.lv7_project_code == wbs_task_lv8.lv8_project_code, wbs_task_lv7.lv7_id == wbs_task_lv8.lv8_upper_id], how='inner')
        .select(
            F.col("lv8_project_code").alias("project_code"),
            F.lit("8").alias("lv"),
            F.concat("lv1_code", F.lit("."), "lv2_code", F.lit("."), "lv3_code", F.lit("."), "lv4_code", F.lit("."), "lv5_code", F.lit("."), "lv6_code", F.lit("."), "lv7_code", F.lit("."), "lv8_code").alias("code"),
            F.col("lv8_name").alias("name"),
            F.col("lv8_id").alias("id"),
            F.col("lv8_upper_id").alias("upper_id"),
            F.col("lv8_yn_leaf").alias("yn_leaf"),
            F.col("lv8_target_begin_date").alias("target_begin_date"),
            F.col("lv8_target_end_date").alias("target_end_date"),
            F.col("lv8_act_begin_date").alias("act_begin_date"),
            F.col("lv8_act_end_date").alias("act_end_date"),
            F.col("lv8_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.col("lv2_id").alias("lv2_id"),
            F.col("lv2_name").alias("lv2_name"),
            F.col("lv3_id").alias("lv3_id"),
            F.col("lv3_name").alias("lv3_name"),
            F.col("lv4_id").alias("lv4_id"),
            F.col("lv4_name").alias("lv4_name"),
            F.col("lv5_id").alias("lv5_id"),
            F.col("lv5_name").alias("lv5_name"),
            F.col("lv6_id").alias("lv6_id"),
            F.col("lv6_name").alias("lv6_name"),
            F.col("lv7_id").alias("lv7_id"),
            F.col("lv7_name").alias("lv7_name"),
            F.col("lv8_id").alias("lv8_id"),
            F.col("lv8_name").alias("lv8_name"),
            F.lit("").alias("lv9_id"),
            F.lit("").alias("lv9_name")
        )
        # .filter(F.col("lv8_id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv8_df = wbs_task_lv8_df.distinct()

    wbs_task_lv9_df = (
        wbs_task_lv1
        .join(wbs_task_lv2, on=[wbs_task_lv1.lv1_project_code == wbs_task_lv2.lv2_project_code, wbs_task_lv1.lv1_id == wbs_task_lv2.lv2_upper_id], how='left')
        .join(wbs_task_lv3, on=[wbs_task_lv2.lv2_project_code == wbs_task_lv3.lv3_project_code, wbs_task_lv2.lv2_id == wbs_task_lv3.lv3_upper_id], how='left')
        .join(wbs_task_lv4, on=[wbs_task_lv3.lv3_project_code == wbs_task_lv4.lv4_project_code, wbs_task_lv3.lv3_id == wbs_task_lv4.lv4_upper_id], how='left')
        .join(wbs_task_lv5, on=[wbs_task_lv4.lv4_project_code == wbs_task_lv5.lv5_project_code, wbs_task_lv4.lv4_id == wbs_task_lv5.lv5_upper_id], how='left')
        .join(wbs_task_lv6, on=[wbs_task_lv5.lv5_project_code == wbs_task_lv6.lv6_project_code, wbs_task_lv5.lv5_id == wbs_task_lv6.lv6_upper_id], how='left')
        .join(wbs_task_lv7, on=[wbs_task_lv6.lv6_project_code == wbs_task_lv7.lv7_project_code, wbs_task_lv6.lv6_id == wbs_task_lv7.lv7_upper_id], how='left')
        .join(wbs_task_lv8, on=[wbs_task_lv7.lv7_project_code == wbs_task_lv8.lv8_project_code, wbs_task_lv7.lv7_id == wbs_task_lv8.lv8_upper_id], how='left')
        .join(wbs_task_lv9, on=[wbs_task_lv8.lv8_project_code == wbs_task_lv9.lv9_project_code, wbs_task_lv8.lv8_id == wbs_task_lv9.lv9_upper_id], how='inner')
        .select(
            F.col("lv9_project_code").alias("project_code"),
            F.lit("9").alias("lv"),
            F.concat("lv1_code", F.lit("."), "lv2_code", F.lit("."), "lv3_code", F.lit("."), "lv4_code", F.lit("."), "lv5_code", F.lit("."), "lv6_code", F.lit("."), "lv7_code", F.lit("."), "lv8_code", F.lit("."), "lv9_code").alias("code"),
            F.col("lv9_name").alias("name"),
            F.col("lv9_id").alias("id"),
            F.col("lv9_upper_id").alias("upper_id"),
            F.col("lv9_yn_leaf").alias("yn_leaf"),
            F.col("lv9_target_begin_date").alias("target_begin_date"),
            F.col("lv9_target_end_date").alias("target_end_date"),
            F.col("lv9_act_begin_date").alias("act_begin_date"),
            F.col("lv9_act_end_date").alias("act_end_date"),
            F.col("lv9_total_float").alias("total_float"),
            F.col("lv1_id").alias("lv1_id"),
            F.col("lv1_name").alias("lv1_name"),
            F.col("lv2_id").alias("lv2_id"),
            F.col("lv2_name").alias("lv2_name"),
            F.col("lv3_id").alias("lv3_id"),
            F.col("lv3_name").alias("lv3_name"),
            F.col("lv4_id").alias("lv4_id"),
            F.col("lv4_name").alias("lv4_name"),
            F.col("lv5_id").alias("lv5_id"),
            F.col("lv5_name").alias("lv5_name"),
            F.col("lv6_id").alias("lv6_id"),
            F.col("lv6_name").alias("lv6_name"),
            F.col("lv7_id").alias("lv7_id"),
            F.col("lv7_name").alias("lv7_name"),
            F.col("lv8_id").alias("lv8_id"),
            F.col("lv8_name").alias("lv8_name"),
            F.col("lv9_id").alias("lv9_id"),
            F.col("lv9_name").alias("lv9_name")
        )
        # .filter(F.col("lv9_id").isNotNull())
        # .filter(F.col("yn_root") == 'Y')
    )
    # wbs_task_lv9_df = wbs_task_lv9_df.distinct()

    # 07 UNION하여 계층구조 데이터 생성
    wbs_final = (
        wbs_task_lv1_df
        .unionAll(wbs_task_lv2_df)
        .unionAll(wbs_task_lv3_df)
        .unionAll(wbs_task_lv4_df)
        .unionAll(wbs_task_lv5_df)
        .unionAll(wbs_task_lv6_df)
        .unionAll(wbs_task_lv7_df)
        .unionAll(wbs_task_lv8_df)
        .unionAll(wbs_task_lv9_df)
    )

    task2 = project_task.select(
        F.col("project_code").alias("project_code").cast("string"),
        F.col("task_code_1").alias("id"),
        # F.col("wbs_code").cast("string").alias("wbs_code_task"),
        # F.col("task_code").alias("code_task"),
        F.col("task_code"),
        F.col("task_type"),
        F.col("early_end_date").cast("string"),
        F.col("expect_end_date").cast("string"),
        )

    wbs_final = (
        wbs_final
        .join(
            task2,
            on=["project_code",
                "id"
                #wbs_final.upper_id == task2.wbs_code_task,
                #wbs_final.code == task2.code_task
                ],
            how="left"
        )
    )

    wbs2 = project_wbs.select(
        F.col("project_code").cast("string"),
        F.col("wbs_code").alias("id"),
        #F.col("upper_wbs_code").cast("string").alias("upper_id"),
        F.col("wbs_short_name").alias("prj_code"),
        F.col("wbs_name").alias("prj_category"),
        )

    wbs_final = (
        wbs_final
        .join(
            wbs2,
            on=["project_code",
                "id"
                #wbs_final.upper_id == task2.wbs_code_task,
                #wbs_final.code == task2.code_task
                ],
            how="left"
        )
    )

    wbs_final = wbs_final.join(project_task.select(
                'task_code_1',
                'clndr_code'), wbs_final['id'] == project_task.task_code_1, "left")

    wbs_final = wbs_final.join(civilpmis_calendar.select(
                'clndr_code',
                'day_hr_count'), on="clndr_code", how="left")

    wbs_final = (
        wbs_final
        .withColumn(
            "TF",
            (F.col('total_float') / F.col('day_hr_count')).cast('integer')
        )
    )

    # task = project_task.select(
    #     F.col("project_code").cast("string"),
    #     F.col("task_code_1").alias("id"),
    #     F.col("wbs_code").cast("string").alias("upper_id"),
    #     F.col("task_code").alias("code"),
    #     F.col("task_name").alias("name"),
    #     F.lit("N").alias("yn_root"),
    #     F.lit("Y").alias("yn_leaf"),
    #     F.col("target_begin_date").cast("date").alias("target_begin_date"),
    #     F.col("target_end_date").cast("date").alias("target_end_date"),
    #     F.col("act_begin_date").cast("date").alias("act_begin_date"),
    #     F.col("act_end_date").cast("date").alias("act_end_date"),
    #     F.col("total_float_hr_count").alias("total_float")
    # )

    # wbs_final = (
    #     wbs_final
    #     .filter(F.col("code").contains("170373"))
    # )

    return wbs_final
