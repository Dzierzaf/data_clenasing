from pyspark.sql import functions as F

def flag_x(df, column_name):
    """
    Ersetzt in der angegebenen Spalte Leerzeichen (" " oder leere Strings) durch False
    und jedes 'X' oder 'x' durch True.

    Parameter
    ----------
    df : pyspark.sql.DataFrame
        Eingangs-DataFrame.
    column_name : str
        Name der zu bearbeitenden Spalte.

    Rückgabe
    --------
    pyspark.sql.DataFrame
        Neuer DataFrame mit ersetzter Spalte.
    """
    return (
        df.withColumn(
            column_name,
            F.when(
                F.lower(F.trim(F.col(column_name))) == "x",
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )
