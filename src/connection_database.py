from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Iterable, Dict, TypeVar, List, Tuple
import contextlib
from .system_log import SystemLogger
from .psw import host_ssl, dbname, user, password_db, schema

import psycopg2
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_batch, DictCursor
from psycopg2.extensions import connection as PgConnection, cursor as PgCursor

# Configuração tipada para conexão
@dataclass(frozen=True)
class PostgreSQLConfig:
    host: str = host_ssl
    dbname: str = dbname
    user: str = user
    password: str = password_db
    schema: str = schema
    port: int = 5432
    connect_timeout: int = 20
    application_name: str = "PostgreSQLHandler"

T = TypeVar('T', bound='PostgreSQLHandler')

class PostgreSQLHandler:
    """
    Classe robusta para manipulação de operações com banco de dados PostgreSQL.
    Implementa o padrão Repository para operações CRUD com tratamento seguro de erros,
    tipagem estática e gerenciamento de recursos.

    Exemplo de uso:
    >>> config = PostgreSQLConfig(
    ...     host="localhost",
    ...     dbname="mydb",
    ...     user="user",
    ...     password="password"
    ... )
    >>> with PostgreSQLHandler(config) as db:
    ...     db.save_dataframe(df, "my_table")
    """    

    def __init__(self, config: PostgreSQLConfig):
        """
        Inicializa o handler com configuração tipada.

        Args:
            config: Configuração de conexão com o PostgreSQL
        """
        self._config = config
        self._connection: Optional[PgConnection] = None
        self._logger = SystemLogger.configure_logger("PostgreSQLHandler")

    def __enter__(self: T) -> T:
        """Permite uso em context manager (with statement)."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Garante que a conexão seja fechada ao sair do contexto."""
        self.disconnect()
        if exc_type is not None:
            self._logger.error(f"Exception occurred: {exc_type.__name__}: {exc_val}", exc_info=(exc_type, exc_val, exc_tb)) # Não suprimimos exceções intencionalmente
    
    @property
    def connection(self) -> PgConnection:
        """Acesso seguro à conexão com verificação de estado."""
        if self._connection is None or self._connection.closed:
            raise psycopg2.InterfaceError("A conexão não está estabelecida ou está fechada.")
        
        return self._connection
    
    def connect(self) -> None:
        """
        Estabelece conexão com o banco de dados PostgreSQL.
        
        Raises:
            psycopg2.OperationalError: Em caso de falha na conexão
        """

        if self._connection is not None and not self.connection.closed:
            self._logger.warning("✅ Conexão já estabelecida.")
            return

        try:
            self._connection = psycopg2.connect(
                host=self._config.host,
                dbname=self._config.dbname,
                user=self._config.user,
                password=self._config.password,
                port=self._config.port,
                connect_timeout=self._config.connect_timeout,
                application_name=self._config.application_name,
                cursor_factory=DictCursor
            )
            self._logger.info("✅ Conexão com o PostgreSQL estabelecida com sucesso.")

            # Configurações recomendadas para performance
            with self._connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC-3'")
                cursor.execute(f"SET search_path TO {self._config.schema}, vivo")
        
        except psycopg2.Error as e:
            self._logger.error(f"Falha ao conectar ao PostgreSQL: {e}")
            raise psycopg2.OperationalError(f"❌ Conexão falhou: {e}") from e
    
    def disconnect(self) -> None:
        """Fecha a conexão com o banco de dados de forma segura."""
        if self._connection is not None and not self._connection.closed:
            try:
                self._connection.close()
                self._logger.info("🔒 Conexão com o PostgreSQL encerrada.")
            
            except psycopg2.Error as e:
                self._logger.error(f"❌ Erro ao encerrar a conexão: {e}")
            
            finally:
                self._connection = None
    
    @contextlib.contextmanager
    def _get_cursor(self) -> Iterable[PgCursor]:
        """
        Context manager para gerenciamento seguro de cursores.
        
        Yields:
            PgCursor: Cursor configurado para operações no banco
            
        Raises:
            psycopg2.Error: Em caso de erro no banco de dados
        """

        cursor = None

        try:
            cursor = self.connection.cursor ()
            yield cursor
            self.connection.commit()

        except psycopg2.Error as e:
            self.connection.rollback()
            self._logger.error(f"❌ Falha na operação do banco de dados: {e}")
            raise

        finally:
            if cursor is not None:
                cursor.close()

    def _map_pandas_to_postgres_type(self, dtype: str) -> str:
         """
        Mapeia tipos do pandas para tipos do PostgreSQL de forma mais completa.
        
        Args:
            dtype: Tipo de dados do pandas
            
        Returns:
            str: Tipo correspondente no PostgreSQL
        """

         type_mapping: Dict[str, str] = {
            # Tipos numéricos
            'int8': 'SMALLINT',
            'int16': 'SMALLINT',
            'int32': 'INTEGER',
            'int64': 'BIGINT',
            'uint8': 'SMALLINT',
            'uint16': 'INTEGER',
            'uint32': 'BIGINT',
            'uint64': 'NUMERIC(20)',
            'float16': 'REAL',
            'float32': 'REAL',
            'float64': 'DOUBLE PRECISION',

            # Tipos temporais
            'datetime64[ns]': 'TIMESTAMP WITH TIME ZONE',
            'timedelta64[ns]': 'INTERVAL',

            # Tipos booleanos
            'bool': 'BOOLEAN',

            # Tipos de texto
            'object': 'TEXT',
            'string': 'TEXT',

            # Tipos binários
            'bytes': 'BYTEA',                                                 
         }

         return type_mapping.get(str(dtype), 'TEXT')
    
    def _prepare_data_for_insert(self, df: pd.DataFrame) -> List[Tuple[Any, ...]]:
        """
        Prepara os dados do DataFrame para inserção no PostgreSQL com tratamento robusto.
        
        Args:
            df: DataFrame a ser preparado
            
        Returns:
            List[Tuple]: Dados preparados como lista de tuplas
        """
        # 1. Trata todos os valores NaN (incluindo NaT para datetime) e 'None' (string) para Python None     
        df_clean = df.replace({pd.NA: None, pd.NaT: None, "None": None, "": None})

        # 2. Converte o DataFrame limpo diretamente para uma lista de tuplas.
        processed_data = [tuple(row) for row in df_clean.to_records(index=False)]
        
        return processed_data
    
    def table_exists(self, table_name: str) -> bool:
        """
        Verifica se uma tabela existe no schema atual.
        
        Args:
            table_name: Nome da tabela a verificar
            
        Returns:
            bool: True se a tabela existe, False caso contrário
            
        Raises:
            psycopg2.Error: Em caso de erro no banco de dados
        """

        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """

        try:
            with self._get_cursor() as cursor:
                cursor.execute(query, (self._config.schema, table_name))
                return cursor.fetchone()[0]
        
        except psycopg2.Error as e:
            self._logger.error(f"❌ Falha ao verificar a existência da tabela: {table_name} - {e}")
            raise
    
    def create_table_from_dataframe(
            self,
            df: pd.DataFrame,
            table_name: str,
            primary_key: Optional[str] = None,
            indexes: Optional[List[str]] = None,
            if_not_exists: bool = True) -> None:
        """
        Cria uma tabela baseada na estrutura de um DataFrame.
        
        Args:
            df: DataFrame com a estrutura desejada
            table_name: Nome da tabela a ser criada
            primary_key: Coluna(s) para chave primária (opcional)
            indexes: Coluna(s) para criar índices (opcional)
            if_not_exists: Se True, só cria se a tabela não existir
            
        Raises:
            psycopg2.Error: Em caso de erro no banco de dados
            ValueError: Se o DataFrame estiver vazio
        """

        if df.empty:
            raise ValueError("Não é possível criar uma tabela a partir de um DataFrame vazio.")

        if if_not_exists and self.table_exists(table_name):
            self._logger.info(f"✅ A tabela {table_name} já existe, ignorando a criação.")
            return
        
        columns_df = []

        for col, dtype in df.dtypes.items():
            pg_type = self._map_pandas_to_postgres_type(str(dtype))
            col_df = f'"{col}" {pg_type}'
            columns_df.append(col_df)

        if primary_key:
            columns_df.append(f'PRIMARY KEY ("{primary_key}")')

        query = f"""
            CREATE TABLE {"IF NOT EXISTS" if if_not_exists else ""} 
            {self._config.schema}.{table_name} (
                {', '.join(columns_df)}
            )
        """
        
        try:
            with self._get_cursor() as cursor:
                cursor.execute(query)

                # Cria índices se especificado
                if indexes:
                    for col in indexes:
                        index_query = f"""
                            CREATE INDEX IF NOT EXISTS 
                            idx_{table_name}_{col} ON {self._config.schema}.{table_name} ("{col}")
                        """
                    
                        cursor.execute(index_query)

            self._logger.info(f"✅ Tabela {self._config.schema}.{table_name} criada com sucesso.")

        except psycopg2.Error as e:
            self._logger.error(f"❌ Falha ao criar a tabela {table_name}: {e}")
            raise
    
    def save_dataframe(
            self,
            df: pd.DataFrame,
            table_name: str,
            batch_size: int = 2000,
            create_table: bool = False,
            truncate: bool = False) -> int:
        """
        Salva um DataFrame em uma tabela PostgreSQL de forma eficiente.
        
        Args:
            df: DataFrame a ser salvo
            table_name: Nome da tabela de destino
            batch_size: Tamanho do lote para inserções em massa
            create_table: Se True, cria a tabela se não existir
            truncate: Se True, limpa a tabela antes da inserção
            
        Returns:
            int: Número de linhas inseridas
            
        Raises:
            psycopg2.Error: Em caso de erro no banco de dados
            ValueError: Se o DataFrame estiver vazio
        """

        if df.empty:
            raise ValueError("❌ Não é possível salvar um DataFrame vazio.")

        if create_table:
            self.create_table_from_dataframe(df, table_name, if_not_exists=True)

        if truncate:
            self.truncate_table(table_name)

        data = self._prepare_data_for_insert(df)
        columns = [f'"{col}"' for col in df.columns]

        insert_query = sql.SQL("""
            INSERT INTO {schema}.{table} ({columns})
            VALUES ({placeholders})
        """).format(
            schema=sql.Identifier(self._config.schema),
            table=sql.Identifier(table_name),
            columns=sql.SQL(', ').join(map(sql.Identifier, df.columns)),
            placeholders=sql.SQL(', ').join([sql.Placeholder()] * len(df.columns))
        )

        try:
            with self._get_cursor() as cursor:
                execute_batch(cursor, insert_query, data, page_size=batch_size)
                rowcount = len(data)
                self._logger.info(f"✅ {rowcount} linhas inseridas em {self._config.schema}.{table_name}")

                return rowcount
        
        except psycopg2.Error as e:
            self._logger.error(f"Falha ao inserir dados em {table_name}: {e}")
    
    def truncate_table(self, table_name: str) -> None:
        """
        Limpa o conteúdo de uma tabela.
        
        Args:
            table_name: Nome da tabela a ser truncada
            
        Raises:
            psycopg2.Error: Em caso de erro no banco de dados
        """
        
        query = sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
            schema=sql.Identifier(self._config.schema),
            table=sql.Identifier(table_name)
        )

        try:
            with self._get_cursor() as cursor:
                cursor.execute(query)
            self._logger.info(f"Truncated table {self._config.schema}.{table_name}")
        
        except psycopg2.Error as e:
            self._logger.error(f"Falha ao truncar a tabela {table_name}: {e}")
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        """
        Executa uma query SQL e retorna os resultados.
        
        Args:
            query: Query SQL a ser executada
            params: Parâmetros para a query (opcional)
            
        Returns:
            List[Dict]: Resultados como lista de dicionários
            
        Raises:
            psycopg2.Error: Em caso de erro no banco de dados
        """

        try:
            with self._get_cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

        except psycopg2.Error as e:
            self._logger.error(f"Falha na execução da consulta: {e}")
            raise
    
    def dataframe_from_query(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """
        Executa uma query e retorna os resultados como DataFrame.
        
        Args:
            query: Query SQL a ser executada
            params: Parâmetros para a query (opcional)
            
        Returns:
            pd.DataFrame: Resultados como DataFrame
            
        Raises:
            psycopg2.Error: Em caso de erro no banco de dados
        """

        try:
            with self._get_cursor() as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)

        except psycopg2.Error as e:
            self._logger.error(f"Falha na execução da consulta: {e}")
            raise        
    
    def execute_non_query(self, query: str, params: Optional[Tuple] = None) -> None:
        """
        Executa uma query DDL ou DCL que não retorna dados (ex: ALTER TABLE).
        """   
        try:
            with self._get_cursor() as cursor:
                cursor.execute(query, params)
                self._logger.info(f"Comando executado: {query.splitlines()[0][:60]}...")
        
        except psycopg2.Error as e:
            self._logger.error(f"Falha na execução do comando DDL: {e}")