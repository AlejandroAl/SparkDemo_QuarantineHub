package com.quarantine.alex.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType, DateType}

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Ventas Processing App")
      .config("spark.driver.memory","3g")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df_ventas_tienda1 = spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/AGREGAR_NOMBRE_BASE_DATOS")
      .option("dbtable","AGREGAR_NOMBRE_TABLA")
      .option("user","AGREGAR_USUARIO")
      .option("password","AGREGAR_PASSWORD")
      .load()


    val df_ventas_tienda2 = spark.read
      .format("jdbc")
      .option("url","jdbc:postgresql://localhost:5432/AGREGAR_NOMBRE_BASE_DATOS")
      .option("dbtable","agregarSchema.AGREGAR_NOMBRE_TABLA")
      .option("user","AGREGAR_USUARIO")
      .option("password","AGREGAR_PASSWORD")
      .load()

    val df_productos_tienda2 = spark.read
      .format("jdbc")
      .option("url","jdbc:postgresql://localhost:5432/AGREGAR_NOMBRE_BASE_DATOS")
      .option("dbtable","agregarSchema.AGREGAR_NOMBRE_TABLA")
      .option("user","AGREGAR_USUARIO")
      .option("password","AGREGAR_PASSWORD")
      .load()

    val schemaTienda3 = StructType(
      List(
        StructField("idVenta",LongType,false),
        StructField("Producto",StringType,false),
        StructField("Total",DoubleType,false),
        StructField("Fecha",DateType,false),
      )
    )


    val df_ventas_tienda3 = spark.read
      .schema(schemaTienda3)
      .option("header","false")
      .option("partitionBy","fecha")
      .csv("AGREGAR_RUTA_DONDE_SE_ENCUENTRAN_LOS_DATOS")

    val df_ventas_tienda1_cost =  df_ventas_tienda1.withColumn("costo_total", $"cantidad"* $"costo_por_unidad")
    val df_ventas_tienda2_cost = df_ventas_tienda2
      .join(df_productos_tienda2, df_ventas_tienda2.col("idProduct") === df_productos_tienda2.col("idProduct"))
      .withColumn("costo_total", $"quantityProduct"* $"cost").select($"name".alias("Producto"), $"costo_total", $"dataSale")


    val df_ventas_tienda1_cost_group = df_ventas_tienda1_cost.groupBy($"producto", $"fecha_venta").sum("costo_total")
      .withColumn("Fecha", df_ventas_tienda1_cost("fecha_venta").cast(DateType)).select($"producto".alias("Producto"),$"Fecha",$"sum(costo_total)".alias("Total"))
    val df_ventas_tienda2_cost_group = df_ventas_tienda2_cost.groupBy($"producto", $"dataSale").sum("costo_total")
      .withColumn("Fecha", df_ventas_tienda2_cost("dataSale").cast(DateType)).select($"producto".alias("Producto"),$"Fecha",$"sum(costo_total)".alias("Total"))
    val df_ventas_tienda3_group = df_ventas_tienda3.groupBy($"Producto", $"Fecha").sum("Total").withColumnRenamed("sum(Total)", "Total")

    val dfDataJoined = df_ventas_tienda1_cost_group.union(df_ventas_tienda2_cost_group).union(df_ventas_tienda3_group)

    val dfDataJoined_metrics = dfDataJoined.groupBy($"Producto", $"Fecha").sum("Total").select($"Producto", $"Fecha",$"sum(Total)".alias("Total"))


    dfDataJoined_metrics.coalesce(1).write.option("header","true").mode("overwrite")
      .csv("RUTA_DONDE_SE_ALMACENARAN_LOS_DATOS")

    spark.close()

  }

}
