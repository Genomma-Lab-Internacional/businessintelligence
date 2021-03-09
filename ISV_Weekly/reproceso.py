import pandas as pd

# Código reproceso para los datos que se almacenan en AWS
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
        A['ID'] = A['ProPstID'].astype(str) + A['SucID'].astype(str) + A['TmpID'].astype(str)
        B['ID'] = B['ProPstID'].astype(str) + B['SucID'].astype(str) + B['TmpID'].astype(str)
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

# Código reproceso para los datos que se almacenan en el DWH
class reproceso_DWH(reproceso_AWS):
    def __init__(self, df_A, df_B):
        super().__init__(df_A, df_B)

    # Función main, donde corre los métodos que se heredan de reproceso_AWS
    def reprocessing(self):
        df_A, df_B = self._prepare_data()
        dcolumns = {'index':[('ProPstID', 'ProPstID_x', 'ProPstID_y'), ('SucID', 'SucID_x', 'SucID_y'), ('TmpID', 'TmpID_x', 'TmpID_y'), ('MonID', 'MonID_x', 'MonID_y')],
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

# Código reproceso para el visor de Colombia
class reproceso_COL(reproceso_AWS):
    def __init__(self, df_A, df_B):
        super().__init__(df_A, df_B)

    # Generación de ID
    def _gen_ids(self, A, B, num_cols, str_cols):
        for col in num_cols:
            A[col] = self._nums_to_string(A, col)
            B[col] = self._nums_to_string(B, col)
        A['ID'] = A['Año_GL'].astype(str) + A['Semana_GL'].astype(str) + A['Canal'] + A['GrpNombre'] + A['ProPstCodBarras'].astype(str)
        B['ID'] = B['Año_GL'].astype(str) + B['Semana_GL'].astype(str) + B['Canal'] + B['GrpNombre'] + B['ProPstCodBarras'].astype(str)
        return A, B

    # Creamos un pivot table con los datos y su ID
    def _prepare_data(self):
        table_A = self.df_A.pivot_table(index=['Año_GL', 'Semana_GL', 'Canal', 'GrpNombre', 'MrcNombre','AgrProNombre', 'ProPstCodBarras', 'ProPstID', 'ProPstNombre'], values=['Sellout unidades', 'Inventario Unidades', 'Sellout $$$', 'Inventario $$$'], aggfunc='sum').reset_index()
        table_B = self.df_B.pivot_table(index=['Año_GL', 'Semana_GL', 'Canal', 'GrpNombre', 'MrcNombre','AgrProNombre', 'ProPstCodBarras', 'ProPstID', 'ProPstNombre'], values=['Sellout unidades', 'Inventario Unidades', 'Sellout $$$', 'Inventario $$$'], aggfunc='sum').reset_index()
        table_A, table_B = self._gen_ids(table_A, table_B, num_cols=['Año_GL', 'Semana_GL', 'ProPstCodBarras'], str_cols=['Canal', 'GrpNombre'])
        return table_A, table_B

    # Función main, donde corre los métodos que se heredan de reproceso_AWS
    def reprocessing(self):
        df_A, df_B = self._prepare_data()
        dcolumns = {'index':[('Año_GL', 'Año_GL_x', 'Año_GL_y'), ('Semana_GL', 'Semana_GL_x', 'Semana_GL_y'), ('Canal', 'Canal_x', 'Canal_y'), ('GrpNombre', 'GrpNombre_x', 'GrpNombre_y'), ('MrcNombre', 'MrcNombre_x', 'MrcNombre_y'), ('AgrProNombre', 'AgrProNombre_x', 'AgrProNombre_y'), ('ProPstCodBarras', 'ProPstCodBarras_x', 'ProPstCodBarras_y'), ('ProPstID', 'ProPstID_x', 'ProPstID_y'), ('ProPstNombre', 'ProPstNombre_x', 'ProPstNombre_y')],
            'sellout':[('Sellout unidades', 'Sellout unidades_x', 'Sellout unidades_y'), ('Sellout $$$', 'Sellout $$$_x', 'Sellout $$$_y')],
            'stock':[('Inventario Unidades', 'Inventario Unidades_x', 'Inventario Unidades_y'), ('Inventario $$$', 'Inventario $$$_x', 'Inventario $$$_y')]}
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
        data_filled = data_filled[['Año_GL', 'Semana_GL', 'Canal', 'GrpNombre', 'MrcNombre','AgrProNombre', 'ProPstCodBarras', 'ProPstID', 'ProPstNombre', ]]
        return data_filled