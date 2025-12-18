from pathlib import Path
from platformdirs import user_downloads_dir

from typing import Optional
import pandas as pd
from dataclasses import dataclass

from .system_log import SystemLogger
import warnings

@dataclass
class FileProcessingResult:
    sucess: bool
    message: str
    dataframe: Optional[pd.DataFrame] = None
    chunks_processed: int = 0


class ExcelFileHendler:
    """
        Handler para processamento de arquivos Excel com prefixo específico.
    
        Attributes:
            directory (Path): Diretório para busca dos arquivos
            prefix (str): Prefixo dos arquivos a serem processados
            column_mapping (Dict[str, str]): Mapeamento de colunas para renomeação
            date_columns (Tuple[str, ...]): Colunas que devem ser tratadas como datas
    """    
    COLUMN_MAPPING = {
        'Id 4Field': 'id_4field',
        'Criação do NTT': 'criacao_do_ntt',
        'Tempo de Abertura': 'tempo_de_abertura',
        'Data': 'data',
        'Hora de criação da atividade (aux)': 'hora_de_criacao_da_atividade_aux',
        'ID da Atividade': 'id_da_atividade',
        'Número de Ordem': 'numero_de_ordem',
        'Evento': 'evento',
        'Tipo da Atividade': 'tipo_da_atividade',
        'Estado': 'estado',
        'Provedor': 'provedor',
        'Matrícula do Provedor': 'matricula_do_provedor',
        'Tipo Contrato': 'tipo_contrato',
        'Regional': 'regional',
        'Contrato': 'contrato',
        'Empresa': 'empresa',
        'UF': 'uf',
        'Cidade': 'cidade',
        'Usuário Executor': 'usuario_executor',
        'ETA': 'eta',
        'Fim': 'fim',
        'Título do Alarme': 'titulo_do_alarme',
        'Tipo da Falha': 'tipo_da_falha',
        'CM': 'cm',
        'END_ID': 'end_id',
        'NE ID': 'ne_id',
        'Tipo de NE': 'tipo_de_ne',
        'BSC/RNC': 'bsc_rnc',
        'Motivo do Pendenciamento': 'motivo_do_pendenciamento',
        'Operadora': 'operadora',
        'Nota de Abertura': 'nota_de_abertura',
        'Prioridade': 'prioridade',
        'Responsabilidade': 'responsabilidade',
        'Sub Área': 'sub_area',
        'Motivo da Suspensão': 'motivo_da_suspensao',
        'Motivo da Tramitação': 'motivo_da_tramitacao',
        'Tramitação ou Suspensão': 'tramitacao_ou_suspensao',
        'Repetido': 'repetido',
        'Início GMG': 'inicio_gmg',
        'Término GMG': 'termino_gmg',
        'Status GMG': 'status_gmg',
        'Responsável GMG': 'responsavel_gmg',
        'Descrição GMG': 'descricao_gmg',
        'Priorização Dispatching': 'priorizacao_dispatching',
        'Priorização Dispatching Classific.': 'priorizacao_dispatching_classific',
        'Workzone': 'workzone',
        'Workzone END_ID': 'workzone_end_id',
        'Data Primeira Roteirização': 'data_primeira_roteirizacao',
        'Data Última Roteirização': 'data_ultima_roteirizacao',
        'Classificação GSBI': 'classificacao_gsbi',
        'Seguimento de rede equipamento': 'seguimento_de_rede_equipamento',
        'Função do Equipamento': 'funcao_do_equipamento',
        'Prédios Industriais': 'predios_industriais',
        'Regra usuário criador': 'regra_usuario_criador',
        'Grupo': 'grupo',
        'ID do Ticket CA': 'id_do_ticket_ca',
        'Qual foi a causa da falha no elemento?': 'qual_foi_a_causa_da_falha_no_elemento',
        'Onde está o problema?': 'onde_esta_o_problema',
        'O que foi feito para resolver?': 'o_que_foi_feito_para_resolver',
        'Data da Coleta': 'data_da_coleta'         
    }
    PREFIX = "backlog"
    CSV_SIZE_TRESHOLD = 10 # Tamanho máximo de arquivo antes de usar chunks (em MB)
    CHUNK_SIZE = 10000 # Número máximo de linhas por chunk
    AVG_LINE_SIZE_KB = 1 # Tamanho estimado por linha (em KB) para cálculo aproximado
    DATETIME_FORMAT = '%d/%m/%Y %H:%M:%S'
    DATETIME_FORMAT_ISO = '%Y-%m-%d %H:%M:%S'
    DATE_COLUMNS = (
        "criacao_do_ntt",
        "hora_de_criacao_da_atividade_aux",
        "eta", 
        "fim",
        "inicio_gmg",
        "termino_gmg",
        "data_primeira_roteirizacao",
        "data_ultima_roteirizacao"        
    )

    def __init__(self, prefix: str = PREFIX, update_time: str = None):
        """
            Inicializa o handler com prefixo.
            
            Args:
                prefix: Prefixo dos arquivos a serem processados
        """        
        self.directory = Path(user_downloads_dir())
        self.prefix = prefix
        self.update_time = update_time
        self.logger = SystemLogger.configure_logger("ExcelFileHendler")
        self._column_types_cache = {} # Cache para tipos de colunas

        warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
        warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
    
    def _find_most_recent_file(self) -> Optional[Path]:
        """
            Encontra o arquivo mais recente com o prefixo configurado.
            
            Returns:
                Path do arquivo mais recente ou None se não encontrado
                
            Raises:
                FileNotFoundError: Se nenhum arquivo for encontrado
        """    
        search_pattern = f"{self.prefix}*"
        files = list(self.directory.glob(search_pattern))

        if not files:
            self.logger.error(f"❌ Nenhum arquivo encontrado com o prefixo: {self.prefix}")
            raise FileNotFoundError(f"❌ Nenhum arquivo com prefixo {self.prefix} encontrado em {self.directory}")
        
        return max(files, key=lambda f: f.stat().st_mtime)
    
    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
            Processa o dataframe com transformações necessárias.
            
            Args:
                df: DataFrame original
                
            Returns:
                DataFrame processado
        """
        # Renomeia colunas
        df = df.rename(columns=self.COLUMN_MAPPING)

        if self.update_time:
            self.logger.info(f"ℹ️ INSERINDO update_time: {self.update_time}")
            df.insert(0, "update_time", self.update_time)
        else:
            self.logger.warning("⚠️ Nenhum update_time fornecido")
        
        for col in self.DATE_COLUMNS:
            if col in df.columns:
                try:
                    # Converte a string original para objeto datetime do Pandas
                    df[col] = pd.to_datetime(df[col], format=self.DATETIME_FORMAT, errors='coerce', exact=False)

                    # Converte o objeto datetime para string no formato ISO (yyyy-mm-dd hh:mm:ss)
                    df[col] = df[col].dt.strfitme(self.DATETIME_FORMAT_ISO)
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Erro na formatação ISO da coluna {col}: {e}")

        # Tratamento de IDs e tipagem Segura
        id_cols = ["id_4field", "id_da_atividade"]    

        for col in id_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype('Int64').astype(str).replace('0', None)

        # Tratamento de outro campo string
        df["priorizacao_dispatching"] = df["priorizacao_dispatching"].astype(str)

        # Limpeza Final (Substitui 'NaT' gerado pelo strftime por None)
        df = df.replace({pd.NA: None, "nan": None, "None": None, "": None, "NaT": None})
                        
        # Normalização de colunas de texto
        text_cols = df.select_dtypes(include=['object']).columns
        df[text_cols] = df[text_cols].astype(str).replace("None", None)

        return df
    
    def _load_small_csv(self, file_path: Path) -> FileProcessingResult:
        """Carrega arquivos CSVs pequenos."""
        try:
            encodings = ['latin-1', 'utf-8']
            df = None

            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=';', on_bad_lines='warn')
                    break
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
            
            if df is None:
                raise ValueError("❌ Não foi possível ler o CSV com os encodings suportados.")

            process_df = self._process_dataframe(df)
            self.logger.info(f"✅ Arquivo {file_path.name} processado com sucesso.")

            return FileProcessingResult(sucess=True, message="✅ Arquivo processado com sucesso.", dataframe=process_df)
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao processar arquivo pequeno: {str(e)}")
            return FileProcessingResult(sucess=False, message=f"❌ Erro: {str(e)}")

    def _load_large_csv(self, file_path: Path) -> FileProcessingResult:
        """Carrega um CSV grande em chunks com tentativa de encoding em cascata."""
        chunks = []

        # Tenta os encodings mais comuns para sistemas brasileiros
        encoding_to_try = ['latin-1', 'utf-8']
        successful_encoding = None

        for encoding in encoding_to_try:
            try:
                # 1. Primeira passada rápida: Inferir tipos e validar encoding
                with pd.read_csv(file_path, encoding=encoding, sep=';', chunksize=1000) as reader:
                    sample = next(reader)
                    self._column_types_cache = sample.dtypes.to_dict()
                    successful_encoding = encoding
                    break
            except (UnicodeDecodeError, StopIteration):
                continue
        
        if not successful_encoding:
            return FileProcessingResult(sucess=False, message="⚠️ Não foi possível detectar o encoding do CSV.")
        
        self.logger.info(f"⚙️ Processando arquivo com encoding: {successful_encoding}")

        try:
            # 2. Segunda passada: Processamento real de TODO o arquivo
            total_rows = 0
            with pd.read_csv(file_path, encoding=successful_encoding, sep=';', chunksize=self.CHUNK_SIZE, dtype=self._column_types_cache, low_memory=False) as reader:
                for chunk in reader:
                    processed_chunk = self._process_dataframe(chunk)
                    chunks.append(processed_chunk)
                    total_rows += len(chunk)

                    if total_rows % (self.CHUNK_SIZE * 5) == 0:
                        self.logger.info(f"⏳ Progresso: {total_rows} linhas processadas...")
            
            df = pd.concat(chunks, ignore_index=True)
            self.logger.info(f"✅ Sucesso: {total_rows} linhas consolidadas em {len(chunks)} chunks.")

            return FileProcessingResult(sucess=True, message=f"✅ Processado com sucesso: {total_rows} linhas", dataframe=df, chunks_processed=len(chunks))
        
        except Exception as e:
            self.logger.error(f"❌ Falha no processamento de chunks: {e}")
            return FileProcessingResult(sucess=False, message=f"❌ Erro no processamento: {str(e)}")

    def _should_use_chunks(self, file_path: Path) -> bool:
        """Determina se deve usar processamento em chunks."""
        file_size = file_path.stat().st_size

        # Verifica por tamanho (10MB)
        if file_size > self.CSV_SIZE_TRESHOLD * 1024 * 1024:
            return True
        
        # Verifica por número estimado de linhas
        estimated_lines = file_size / (self.AVG_LINE_SIZE_KB * 1024)
        return estimated_lines > self.CHUNK_SIZE
    
    def _load_to_dataframe(self, file_path: Path, update_time: str = None) -> FileProcessingResult:
        """
            Carrega arquivo Excel em DataFrame com tratamento de erros.
            
            Args:
                file_path: Path do arquivo a ser carregado
                
            Returns:
                FileProcessingResult com status e dados
        """        
        # Usar o update_time passado como parâmetro se existir, senão usar o da instância
        effective_update_time = update_time if update_time is not None else self.update_time
        self.update_time = effective_update_time

        try:
            self.logger.info(f"⌛ Carregando arquivo: {file_path.name}")

            # Verifica tamanho do arquivo para decidir estratégia
            if self._should_use_chunks(file_path):
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                estimated_lines = int(file_path.stat().st_size / self.AVG_LINE_SIZE_KB * 1024)

                self.logger.info(f"ℹ️ Arquivo grande detectado - Tamanho: {file_size_mb:.2f} MB, Linhas estimadas: {estimated_lines} - usando processamento em chunks")
                return self._load_large_csv(file_path)
            else:
                self.logger.info(f"ℹ️ Processando como arquivo pequeno: ({file_size_mb:.2f} MB)")
                return self._load_small_csv(file_path)
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao processar arquivo {file_path.name}: {e}")
            return FileProcessingResult(sucess=False, message=f"❌ Erro ao processar arquivo: {str(e)}")

    def process_most_recent_file(self) -> FileProcessingResult:
        """
            Processa o arquivo mais recente encontrado.
            
            Returns:
                FileProcessingResult com status e dados
        """        
        try:
            file_path = self._find_most_recent_file()
            return self._load_to_dataframe(file_path)
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao processar arquivo mais recente: {e}")
            return FileProcessingResult(sucess=False, message=f"❌ Erro ao processar arquivo mais recente: {str(e)}")