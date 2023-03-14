from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, configure


@configure(profile=["EXECUTOR_MEMORY_MEDIUM", "DRIVER_MEMORY_EXTRA_LARGE"])
@transform_df(
    Output("ri.foundry.main.dataset.eefd2171-5642-42e9-9009-e8bfaa035dbf"),
    wbs_fin=Input("ri.foundry.main.dataset.af773ed6-9223-41fa-a5cb-05f1db81be25"),
    civil_obs=Input("ri.foundry.main.dataset.1cab4c34-7497-4a25-962d-3190afa922d0"),
    civil_obs_project=Input("ri.foundry.main.dataset.1377a74c-86fd-4586-b4e6-58710fd82df9"),
    mdm_project_master=Input("ri.foundry.main.dataset.06f9c5d7-a098-4360-8713-c07d66ad2286"),
    project_task=Input("ri.foundry.main.dataset.014dbc54-c423-4e4f-aa19-ae6926203887"),
    civilpmis_project=Input("ri.foundry.main.dataset.1b79de17-341f-4831-b71e-0d40ff6549da")
)
def compute(civil_obs, civil_obs_project, mdm_project_master, wbs_fin, civilpmis_project, project_task):

    civil_obs = (
        civil_obs.withColumn(
                    'obs_code',
                    (
                        (F.col('obs_code').cast("integer")).cast('String')
                        )
                )
        .withColumn(
                            'upper_obs_code',
                            (
                                (F.col('upper_obs_code').cast("integer")).cast('String')
                                )
                        )
        .withColumn(
                            'number_seq',
                            (
                                (F.col('number_seq').cast("integer"))
                                )
                        )
            )

    civil_obs = civil_obs.join(civil_obs_project, on='obs_code', how='inner')

    civil_obs = civil_obs.filter(F.col("upper_obs_code").isNotNull())

    civil_obs = (
        civil_obs.withColumn(
                    'project_code',
                    (
                        (F.col('project_code').cast("integer")).cast('String')
                    )
            )
        .withColumn(
                            'wbs_code',
                            (
                                (F.col('wbs_code').cast("integer")).cast('String')
                            )
                    )
        .withColumn("obs_name", F.regexp_replace(F.col("obs_name"), "-1", ""))
         )

    wbs_fin = wbs_fin.withColumnRenamed('project_code', 'project_code_wbs')

    civilp6_obs_join_wbs = civil_obs.join(wbs_fin, civil_obs.project_code == wbs_fin.project_code_wbs, "inner")

    civilp6_obs_join_wbs = civilp6_obs_join_wbs.join(mdm_project_master.select(
                'site_code',
                'site_name'), civil_obs.obs_name == mdm_project_master.site_code, "inner")

    civilp6_obs_join_wbs = civilp6_obs_join_wbs.filter(F.col("site_name").isNotNull())

    civilpmis_project = (
        civilpmis_project
        .withColumnRenamed('creation_date', 'creation_date_project')
        .withColumnRenamed('update_date', 'update_date_project')
    )

    civilp6_obs_join_wbs = civilp6_obs_join_wbs.join(civilpmis_project.select('project_code',
                                                                              "add_date",
                                                                              "update_date_project",
                                                                              "creation_date_project"), ['project_code'], "inner")

    civilp6_obs_join_wbs = (
        civilp6_obs_join_wbs
        .withColumn(
                            'id',
                            (
                                F.col('id').cast("string")
                            )
                    )
        .withColumn(
                            'lv1_id',
                            (
                                F.col('lv1_id').cast("string")
                            )
                    )
        .withColumn(
                            'early_end_date',
                            (
                                F.col('early_end_date').cast("date")
                            )
                    )
        .withColumn(
           "task_label",
            (
                    F.when(
                        (
                            (F.col("task_code").isin('A00000', 'A90000', 'Z10000', 'Z90000'))
                            ),
                        F.lit('y')).otherwise(F.lit("n"))
                    )
        )
        .withColumn(
           "activity_type",
            (
                    F
                    .when((F.col("task_code") == 'A00000'), '계약_착공일')
                    .when((F.col("task_code") == 'A90000'), '최초_계약종료일')
                    .when((F.col("task_code") == 'Z10000'), '목표계약종료일')
                    .when((F.col("task_code") == 'Z90000'), '예상종료일')
                    )
                )
            )

    civilp6_obs_join_wbs_temp = civilp6_obs_join_wbs.filter(
        F.col("task_code").startswith("A90") & (F.col("task_type") == 'TT_FinMile'))

    civilp6_obs_join_wbs = civilp6_obs_join_wbs.filter(F.col("task_label") == 'y')

    task_max = (
        civilp6_obs_join_wbs_temp
        .groupBy("project_code")
        .agg(F.max(F.col("task_code")))
    )

    task_max = task_max.withColumnRenamed('max(task_code)', 'task_code')

    df = civilp6_obs_join_wbs_temp.join(task_max, on=["project_code", "task_code"], how="inner")

    df = df.withColumn(
        'activity_type',
        (
            F.when((F.col("task_label") == 'n'), '변경계약종료일')
            )
    )

    df = (
        df
        .unionByName(civilp6_obs_join_wbs)
    )

    df = (
        df
        .withColumn(
            'class',
            F.when(
                F.col('lv1_name').contains('[Baseline]'), 'Baseline'
                )
            .when(
                F.col('lv1_name').contains('[전월]'), '전월'
                )
            .when(
                F.col('lv1_name').contains('[당월]') | F.col('lv1_name').contains('[금월]'), '당월'
                )
            .otherwise('')
            )
        )

    df = (
        df
        .withColumn(
            'class_code',
            (
                F.concat_ws('_', df.activity_type, df['class'])
             )
        )
    )

    df = (
        df
        .filter(F.col("class").isNotNull())
        .filter(F.col("site_name") != 'GTX-A사업단 현장')
        # .filter(F.col("class_code").isin('계약_착공일_Baseline', '변경계약종료일_당월', '예상종료일_Baseline', '예상종료일_전월', '예상종료일_당월'))
    )

    df = (
            df.withColumn(
                "date",
                (
                    F
                    .when(F.col('class_code') == '계약_착공일_Baseline', F.col('target_begin_date'))
                    .when(F.col('class_code') == '계약_착공일_당월', F.col('target_begin_date'))
                    .when(F.col('class_code') == '계약_착공일_전월', F.col('target_begin_date'))
                    .when(F.col('class_code') == '최초_계약종료일_Baseline', F.col('target_end_date'))
                    .when(F.col('class_code') == '변경계약종료일_당월', F.col('target_end_date'))
                    .when(F.col('class_code') == '예상종료일_Baseline', F.col('target_end_date'))
                    .when(F.col('class_code') == '예상종료일_전월', F.col('target_end_date'))
                    .when(F.col('class_code') == '예상종료일_당월', F.col('target_end_date'))
                    )
                )
        )

    df = (
        df
        .filter(F.col("date").isNotNull())
    )

    # df = df.select(
    #                                         "site_code",
    #                                         "site_name",
    #                                         "wbs_code",
    #                                         "date",
    #                                         F.col("lv1_name").alias('project_name'),
    #                                         'class',
    #                                         "code",
    #                                         'class_code',
    #                                         "task_code",
    #                                         "name",
    #                                         "activity_type",
    #                                         "task_type",
    #                                         "project_code",
    #                                         "early_end_date",
    #                                         "target_begin_date",
    #                                         "target_end_date",
    #                                         "act_begin_date",
    #                                         "act_end_date",
    #                                         "total_float",
    #                                         "task_label"
    #                                         )

    df = df.groupBy("site_name").pivot("class_code").agg(F.min("date"))

    df = (
        df.withColumn(
            '계약착공일',
            F.coalesce(
                F.col('계약_착공일_Baseline'),
                F.col('계약_착공일_당월'),
                F.col('계약_착공일_전월')
            )
        )
    )

    df = (
        df.withColumn(
            '계약종료일',
            F.coalesce(
                F.col('변경계약종료일_당월'),
                F.col('최초_계약종료일_Baseline')
            )
        )
    )

    df = df.select(
        "site_name",
        '계약착공일',
        "계약종료일",
        F.col('예상종료일_Baseline').alias('Baseline_종료일'),
        F.col('예상종료일_전월'),
        F.col('예상종료일_당월')
    )

    df = (
        df
        .withColumn(
            "전월대비_예상종료일",
            (
                F.datediff('예상종료일_당월', '예상종료일_전월')
                )
        )
        .withColumn(
            "계약종료일대비",
            (
                F.datediff('예상종료일_당월', '계약종료일')
                )
        )
    )

    return df
