package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                           .appName("TpSparkDataframe")
                           .master("local[*]")
                           .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("src/main/resources/codesPostaux.csv")


  // 1) Montrer le Schema + son type
  df.show()
  df.printSchema()
  println("nb de colonnes : " + df.columns.size)


  // 2) Il y a 35100 communes
  //a 
  println(df.select("Code_commune_INSEE")
  .distinct()
  .count)

  //b
  println(df.select(countDistinct("Nom_commune").alias("Nombre de commune")))

  // 3) Il y a 2191 communes qui ont pour attribut "Line_5"
  //a 
  println(df.select("Code_commune_INSEE")
  .filter("Ligne_5 is not null")
  .distinct()
  .count())

  //b 
  println(df.filter(col("Ligne_5")=!="").select(countDistinct("Nom_commune").alias("Nombre de commune avec Ligne_5")))


  // 4) Ajout de la colonne num département
  //a 
  val df_with_dep = df.withColumn("Numero_departement", column("Code_postal")
                .substr(1,2))

  df_with_dep.show()

  //b
  df_with_dep = df.withColumn("departement",col("Code_postal").substr(1,2))
  println(df_with_dep)

  // 5) Ecriture dans un csv 
  //a 
  df_with_dep.select("Code_commune_INSEE", "Nom_commune", "Code_postal", "Numero_departement")
                      .sort("Code_postal")
                      .write.options("header",true)
                      .csv("src/main/resources/df_with_dep.csv")

    
  //b
  csv = df_with_dep.drop("Ligne_5").drop("Libellé_d_acheminement").drop("coordonnees_gps").sort("Code_postal")
  
  csv.write
           .option("header","true")
           .option("delimiter",";")
           .csv("src/main/resources/df_with_dep2.csv")
  

  // 6) Afficher les communes ou le departement est l'Aisne.
  //a 
  df_with_dep.filter("Numero_departement = 02")
      .show()
 
  //b
  df_with_dep.where(df_with_dep("departement")==="02")

  // 7) Trier les departements par nb de communes
  //a 
  df_with_dep.groupBy("Numero_departement")
             .count()
             .show()

  //b
  println(df_with_dep.groupBy(col("departement")).agg(countDistinct("Nom_commune").as("Nb_commune")).orderBy(desc("Nb_commune")).show(1))

  spark.close

  }

}




