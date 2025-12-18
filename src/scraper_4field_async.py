import asyncio
from playwright.async_api import async_playwright, Playwright, Page, BrowserContext

from platformdirs import user_downloads_dir
from pathlib import Path
from typing import Optional
import time

from src.system_log import SystemLogger
from src.psw import username, password

class Automation4Field:
    """
    Classe principal para automação do acesso ao sistema 4FIELD da Tim.
    
    Attributes:
        login_url (str): URL do sistema
        username (str): Login de rede do usuário
        password (str): Senha do usuário
        browser (Browser): Instância do browser
        context (BrowserContext): Contexto do browser
        page (Page): Página principal
    """     

    def __init__(self):
        """
        Inicializa a classe de automação.
        
        Args:
            username (str): Login de rede do usuário
            password (str): Senha do usuário
        """ 

        self.login_url = "https://4field.timbrasil.com.br/login"
        self.username = username
        self.password = password
        self.logger = SystemLogger.configure_logger('Automation4Field')
        self.selectors = {
            "login_input": "input.resource-id",
            "password_input": "input.senha",
            "submit_button": ".continue",
            "fn_backlog": "//div[h2[normalize-space()='Backlog']]",
            "home_check": "span.backlog_activities_update_time",
            "main_loader": "div.progress_bar",
            "priority_chart": "canvas#consolidated-daily",
            "export_icon": "div.export-content"        
        }
        self.playwright_engine: Playwright = None
        self.context: BrowserContext = None
        self.page: Page = None
        self.download_dir = Path(user_downloads_dir())

    async def _setup_browser(self) -> Page:
        """
        Configuração do browser
        
        Returns:
            Page: Página configurada e pronta
        """

        # Armazena a instância no atributo tipado
        self.playwright_engine = await async_playwright().start()

        # Cria diretório para perfil persistente
        profile_path = Path("chrome_profile_normal")
        profile_path.mkdir(exist_ok=True)

        # ✅ CONTEXTO PERSISTENTE - todas as páginas herdam este perfil
        self.context = await self.playwright_engine.chromium.launch_persistent_context(
            user_data_dir=str(profile_path),
            headless=False,
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            accept_downloads=True,
            ignore_https_errors=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',           # Necessário em ambientes Linux/CI
                '--disable-gpu',          # Reduz o uso de recursos gráficos
                '--disable-dev-shm-usage',# Essencial para execução em Docker/CI
                '--no-default-browser-check' # Otimização de tempo de inicialização
            ]
        )

        # 🛡️ Script de indetectabilidade
        await self.context.add_init_script(
            """
            delete Object.getPrototypeOf(navigator).webdriver;
            window.chrome = { runtime: {} };
            """           
        )

        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()

        self.logger.info("✅ Browser configurado com sucesso")
        return self.page
    
    async def _load_page_coroutines(self, check_elements: list = None):
        """Corotinas para verificação de carregamento"""

        tasks = [
            self.page.wait_for_load_state('networkidle'), # 1️⃣ Rede ociosa
            self.page.wait_for_function("document.readyState === 'complete'") # 2️⃣ DOM completo
        ]

        # 3️⃣ Elementos específicos (opcional)
        if check_elements:
            for selector in check_elements:
                tasks.append(self.page.wait_for_selector(selector, state='visible', timeout=25000))
        
        # 🔄 Executa tudo em paralelo
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return all(not isinstance(result, Exception) for result in results)
    
    async def _wait_for_page(self, step_name: str, timeout: int = 90, check_elements: list = None) -> bool:
        """
        🚀 Aguardar carregamento completo
        
        Args:
            step_name: Nome da etapa para logs
            timeout: Timeout total em segundos (não cumulativo)
            check_elements: Lista de seletores para verificar (opcional)
        """   

        self.logger.info(f"🌐 Aguardando carregamento: {step_name}") 
        start_time = time.time()

        try:
            # ⚡ Estratégia em paralelo para melhor performance
            sucess = await asyncio.wait_for(self._load_page_coroutines(check_elements), timeout=timeout)

            load_time = time.time() - start_time

            if sucess:
                self.logger.info(f"✅ {step_name} carregado em {load_time:.1f}s")
                return True
            else:
                self.logger.error(f"❌ {step_name} - Alguns elementos não foram carregados")
                return False
        
        except asyncio.TimeoutError:
            self.logger.error(f"⌛ Timeout {timeout}s em: {step_name}")

            # Verifica se algum elemento crítico está presente mesmo com timeout
            if check_elements:
                for selector in check_elements:
                    try:
                        if await self.page.locator(selector).count() > 0:
                            self.logger.info(f"✅ Elemento {selector} encontrado mesmo com timeout")
                            return True
                    
                    except:
                        continue
            
            return False
        
        except Exception as e:
            self.logger.error(f"❌ Erro em {step_name}: {e}")
            return False
    
    async def _wait_for_loader(self, timeout: int = 60000) -> bool:
        """
        Aguarda o desaparecimento do loader e regista o tempo que levou.
        """

        selector = self.selectors.get("main_loader")
        start_time = time.perf_counter() # Início do cronómetro

        self.logger.info(f"⏳ Aguardando processamento da visão ({selector})...")

        try:
            try:
                # --- ETAPA 1: ESPERAR APARECER ---
                await self.page.wait_for_selector(selector, state="visible", timeout=5000)
                self.logger.info("⚡ Loader detectado, processando...")
            
            except Exception as e:
                # Se não aparecer em 5s, o sistema pode ter sido ultra rápido, ou a transição não disparou o loader.
                self.logger.info("ℹ️ Loader não apareceu no tempo limite (possível resposta rápida).")
                return True
        
            # --- ETAPA 2: ESPERAR SUMIR ---
            await self.page.wait_for_selector(selector, state="hidden", timeout=timeout)
            
            end_time = time.perf_counter() # Fim do cronómetro
            duration = end_time - start_time
            self.logger.info(f"✅ Loader finalizado em {duration:.2f} segundos.")
            
            return True
        
        except Exception as e:
            end_time = time.perf_counter()
            duration = end_time - start_time
            self.logger.error(f"❌ Timeout: O loader não desapareceu após {duration:.2f}s. Erro: {e}")
            return False        
    
    async def _safe_fill(self, selector_key: str, value: str) -> bool:
        """
        Valida a existência e visibilidade de um campo antes de preenchê-lo.
        
        Args:
            selector_key: A chave do seletor no dicionário self.selectors
            value: O valor a ser preenchido (senha ou usuário)
        """ 

        selector = self.selectors.get(selector_key)

        try:
            # O Playwright já garante a 'actionability' (estável, visível, habilitado) antes de preencher.
            await self.page.locator(selector).fill(value)
            self.logger.info(f"✅ Campo '{selector_key}' preenchido.")
            return True
        except Exception as e:
            self.logger.error(f"❌ Erro ao interagir com '{selector_key}' ({selector}): {e}")
            return False

    async def _login(self) -> bool:
        """Executa o processo completo de login"""

        try:
            # 1.⚙️Configuração e Navegação
            page = await self._setup_browser()
            await page.goto(self.login_url)

            # 2.⌛Aguarda a página carregar com os seletores centralizados
            login_ready = await self._wait_for_page(step_name="Página de Login", check_elements=[self.selectors["login_input"], self.selectors["password_input"]])

            if not login_ready:
                return False
            
            # 3.🖊️Preenchimento Validado
            credentials = {
                "login_input": self.username,
                "password_input": self.password
            }

            self.logger.info("🖊️ Preenchendo formulário...")

            for key, val in credentials.items():
                sucess = await self._safe_fill(key, val)
                if not sucess:
                    return False
            
            # 4.🚀Submissão
            await self.page.locator(self.selectors["submit_button"]).click()
            self.logger.info("🚀 Formulário enviado. Aguardando resposta do sistema...")

            # 5.🔍Verificação de Sucesso (Home)
            is_logged = await self._wait_for_page(step_name='Página de Boas Vindas', check_elements=[self.selectors["home_check"]])
            
            if is_logged:
                self.logger.info("✅ Login realizado com sucesso.")
                return True
            
            return False
        except Exception as e:
            self.logger.error(f"❌ Falha no login: {e}")
            return False
    
    async def _export_data(self) -> Optional[Path]:
        
        await self.page.locator(self.selectors["fn_backlog"]).click()

        try:
            # ⌛Aguardando o processamento da visão
            if await self._wait_for_loader():
                sucess = await self._wait_for_page(
                    step_name="Visão Backlog", 
                    check_elements=[self.selectors["priority_chart"], self.selectors["export_icon"]]
                    )
                
                if sucess:
                    async with self.page.expect_download(timeout=120000) as download_info:
                        # 🖱️ Clica na imagem de um arquivo Excel para baixar a base
                        await self.page.locator(self.selectors["export_icon"]).click()
                        self.logger.info("✅ Ícone para exportar a base clicado.")

                    # Obtendo o objeto Download
                    download = await download_info.value

                    final_name = f"{download.suggested_filename}"
                    final_path = self.download_dir / final_name

                    await download.save_as(str(final_path))
                    self.logger.info(f"💾 Download salvo em: {final_path}")

                    if await self._validate_download_file(final_path):
                        self.logger.info("🎉 Exportação concluída e validada com sucesso!")
                        return final_path
                    
                    await download.delete()
                    return None
                
        except Exception as e:
            self.logger.error(f"❌ Erro durante exportação: {e}")
            return None
    
    async def _validate_download_file(self, file_path: Path) -> bool:

        try:
            if not file_path.exists():
                return False
            
            file_size = file_path.stat().st_size

            if file_size == 0:
                self.logger.warning("❌ Arquivo vazio")
                return False
            
            with open(file_path,'r', encoding='latin-1') as f:
                if bool(f.readline()) and (f.readline()):
                    self.logger.info("✅ CSV validado com sucesso") 
                    return True
        
        except Exception as e:
            self.logger.error(f"❌ Falha na leitura do CSV: {e}")
            return False

    async def close(self):
        """Fecha o browser e encerra o motor do Playwright de forma limpa"""

        try:
            if self.context:
                await self.context.close()
                self.logger.info("🔒 Contexto e Browser encerrados.")

            if hasattr(self, 'playwright_engine'):
                await self.playwright_engine.stop()
                self.logger.info("🔚 Motor Playwright finalizado.")
        
        except Exception as e:
            self.logger.error(f"⚠️ Erro ao fechar o browser: {e}")
    

    async def execute_process_4field(self):

        if await self._login():
            await self._export_data()
            return True


if __name__ == '__main__':

    async def main():

        scraper = Automation4Field()
        
        try:
            sucess = await scraper.execute_process_4field()

            if sucess:
                print("✅ Processo concluído com sucesso!")
                await asyncio.sleep(3)
            else:
                print("❌ Falho no login")
        finally:
            await scraper.close()

    asyncio.run(main())