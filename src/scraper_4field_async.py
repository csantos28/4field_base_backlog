import asyncio
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

from platformdirs import user_downloads_dir
from pathlib import Path
from typing import Optional, Tuple
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
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None
        self.download_dir = Path(user_downloads_dir())

    async def _setup_browser(self) -> Page:
        """
        Configuração do browser
        
        Returns:
            Page: Página configurada e pronta
        """

        # 🚀 Inicialização direta
        playwright = await async_playwright().start()

        # Cria diretório para perfil persistente
        profile_path = Path("chrome_profile_normal")
        profile_path.mkdir(exist_ok=True)

        # ✅ CONTEXTO PERSISTENTE - todas as páginas herdam este perfil
        self.context = await playwright.chromium.launch_persistent_context(
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
                tasks.append(self.page.wait_for_selector(selector, state='visible', timeout=15000))
        
        # 🔄 Executa tudo em paralelo
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return all(not isinstance(result, Exception) for result in results)
    
    async def _wait_for_page(self, step_name: str, timeout: int = 60, check_elements: list = None) -> bool:
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
    
    async def _login(self) -> bool:
        """Executa o processo completo de login"""

        try:
            # Configuração inicial
            page = await self._setup_browser()
            await page.goto(self.login_url)
            await self._wait_for_page(step_name="Página de Login", check_elements=["input.resource-id", "input.senha"])
            
            # Identificando os elementos de login
            try:
                username_field = self.page.locator("input.resource-id")
                password_field = self.page.locator("input.senha")

                #⚡Aguarda TODOS em PARALELO
                await asyncio.gather(
                    username_field.wait_for(state='visible', timeout=15000),
                    password_field.wait_for(state='visible', timeout=15000)
                )

                self.logger.info("✅ Todos elementos de login localizados")
            
            except Exception as e:
                self.logger.error(f"❌ Falha ao localizar elementos: {e}")
                return False
            
            self.logger.info("🖊️ Preenchendo formulário...")

            try:
                await username_field.fill(self.username)
                self.logger.info("✅ Usuário preenchido")

                await password_field.fill(self.password)
                self.logger.info("✅ Senha preenchida")

                await self.page.locator(".continue").click()
                self.logger.info("✅ Formulário submetido")
            
            except Exception as e:
                self.logger.error(f"❌ Erro no preenchimento: {e}")
                return False
            
            return True
        
        except Exception as e:
            self.logger.error(f"❌ Falha no login: {e}")
    
    async def close(self):
        """Fecha o browser"""

        if self.context:
            await self.context.close
            self.logger.info("🔚 Browser fechado.")


if __name__ == '__main__':

    async def main():

        scraper = Automation4Field()
        
        try:
            sucess = await scraper._login()

            if sucess:
                print("✅ Processo concluído com sucesso!")
                await asyncio.sleep(5)
            else:
                print("❌ Falho no login")
        finally:
            await scraper.close()

    asyncio.run(main())