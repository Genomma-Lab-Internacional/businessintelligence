import pandas as pd

class reproceso_AWS():
    # df_A: DataFrame con los datos anteriores
    # df_B: DataFrame con los datos nuevos. Los datos que se procesarán.
    def __init__(self, df_A, df_B):
        self.df_A = df_A
        self.df_B = df_B
    
    # Transformación de columnas a string
    def _nums_to_string(self, df, column):    
        df[column] = df[column].astype('int64')
        df[column] = df[column].map(str)
        return df[column]

    # Generación de ID
    def _gen_ids(self, A, B, num_cols):
        for col in num_cols:
            A[col] = self._nums_to_string(A, col)
            B[col] = self._nums_to_string(B, col)
        A['ID'] = A[num_cols].sum(axis=1)
        B['ID'] = B[num_cols].sum(axis=1)
        return A, B

    # Creamos un pivot table para evitar duplicidad en el ID
    def _prepare_data(self):
        table_A = pd.pivot_table(self.df_A, index=['ProPstID', 'SucID', 'TmpID'], values=['SoutCantDesp', 'SoutCantExist', 'SoutCantCedis', 'SoutCantTrans', 'SoutCantStaple', 'SoutCantInv', 'SoutMontoDesp', 'SoutMontoExist', 'SoutMontoCedis', 'SoutMontoTrans', 'SoutMontoStaple', 'SoutMontoInv', 'SoutMontoCteDesp'], aggfunc='sum').reset_index()
        table_B = pd.pivot_table(self.df_B, index=['ProPstID', 'SucID', 'TmpID'], values=['SoutCantDesp', 'SoutCantExist', 'SoutCantCedis', 'SoutCantTrans', 'SoutCantStaple', 'SoutCantInv', 'SoutMontoDesp', 'SoutMontoExist', 'SoutMontoCedis', 'SoutMontoTrans', 'SoutMontoStaple', 'SoutMontoInv', 'SoutMontoCteDesp'], aggfunc='sum').reset_index()
        table_A, table_B = self._gen_ids(A=table_A, B=table_B, num_cols=['ProPstID', 'SucID', 'TmpID'])
        return table_A, table_B

    # Función que nos ayuda a elegir el dato no nulo
    def _fill(self, col1, col2):
        if pd.isnull(col2):
            return col1
        elif pd.isnull(col1):
            return col2
        else:
            return col1
            
    # Función para reprocesar los datos de inventario
    def _final_stock(self, stock_old, stock_new):
        if stock_new == 0:
            return stock_old
        else:
            return stock_new

    # Función para reprocesar los datos de ventas
    def _final_sales(self, sales_old, sales_new):
        if pd.isna(sales_new):
            return sales_old
        else:
            return sales_new
    
    # Función para reprocesar los datos no numéricos
    def _fill_column_index(self, df, new_column, column_x, column_y):
        data = pd.DataFrame()
        data[new_column] = df[column_y].combine(df[column_x], self._fill)
        return data

    # Función para reprocesar las columnas de ventas
    def _fill_column_so(self, df, new_column, column_x, column_y):
        data = pd.DataFrame()
        data[new_column] = df[[column_x, column_y]].apply(lambda x: self._final_sales(*x), axis=1)
        return data

    # Función para reprocesar las columnas de stock
    def _fill_column_stock(self, df, new_column, column_x, column_y):
        data = pd.DataFrame()
        data[new_column] = df[[column_x, column_y]].fillna(0).apply(lambda x: self._final_stock(*x), axis=1)
        return data

    # Función main, donde junta todo lo anterior
    def reprocessing(self):
        df_A, df_B = self._prepare_data()
        dcolumns = {'index':[('ProPstID', 'ProPstID_x', 'ProPstID_y'), ('SucID', 'SucID_x', 'SucID_y'), ('TmpID', 'TmpID_x', 'TmpID_y')],
            'sellout':[('SoutCantDesp', 'SoutCantDesp_x', 'SoutCantDesp_y'), ('SoutMontoDesp', 'SoutMontoDesp_x', 'SoutMontoDesp_y'), ('SoutMontoCteDesp', 'SoutMontoCteDesp_x', 'SoutMontoCteDesp_y')],
            'stock':[('SoutCantCedis', 'SoutCantCedis_x', 'SoutCantCedis_y'), ('SoutCantExist', 'SoutCantExist_x', 'SoutCantExist_y'), ('SoutCantInv', 'SoutCantInv_x', 'SoutCantInv_y'), ('SoutCantTrans', 'SoutCantTrans_x', 'SoutCantTrans_y'), ('SoutMontoExist', 'SoutMontoExist_x', 'SoutMontoExist_y'), ('SoutMontoCedis', 'SoutMontoCedis_x', 'SoutMontoCedis_y'), ('SoutMontoTrans', 'SoutMontoTrans_x', 'SoutMontoTrans_y'), ('SoutMontoInv', 'SoutMontoInv_x', 'SoutMontoInv_y'), ('SoutMontoStaple', 'SoutMontoStaple_x', 'SoutMontoStaple_y'), ('SoutCantStaple', 'SoutCantStaple_x', 'SoutCantStaple_y')]}
        join = pd.merge(df_A, df_B, on='ID', how='outer')
        data_filled = pd.DataFrame()
        for k, v in dcolumns.items():
            for i in v:
                if k == 'index':
                    data_filled = pd.concat([data_filled, self._fill_column_index(join, i[0], i[1], i[2])], axis=1)
                elif k == 'sellout':
                    data_filled = pd.concat([data_filled, self._fill_column_so(join, i[0], i[1], i[2])], axis=1)
                elif k == 'stock':
                    data_filled = pd.concat([data_filled, self._fill_column_stock(join, i[0], i[1], i[2])], axis=1)
        return data_filled