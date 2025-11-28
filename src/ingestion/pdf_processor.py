import tabula
import pandas as pd
import logging
from openai import OpenAI
from src.config import OPENROUTER_API_KEY, OPENROUTER_MODEL
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class PDFProcessor:
    def __init__(self):
        if OPENROUTER_API_KEY:
            self.llm_client = OpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=OPENROUTER_API_KEY,
                default_headers={
                    'HTTP-Referer': "https://github.com/congress-analyst",
                    "X-Title": "Congress Trading Analyst"
                }
            )
        else:
            logger.warning("OpenRouter API key missing. LLM features disabled")
            self.llm_client = None


    def extract_tabes_tabula(self, pdf_path: str) -> pd.DataFrame:
        """to get tables using tabula for pdfs and shit"""
        logger.info(f"attempting tabula extraction on {pdf_path}")
        try:
            dfs = tabula.read_pdf(pdf_path, pages="all", multiple_tables=True, lattice=True)

            transaction_dfs = []
            for df in dfs:
                df.columns = [str(c).lower().strip() for c in df.columns]

                if any(k in df.columns for k in ['ticker', 'symbol', 'asset', 'transaction type']):
                    transaction_dfs.append(df)

            if transaction_dfs:
                result = pd.concat(transaction_dfs, ignore_index=True)
                logger.info(f"Tabula found {len(result)} rows.")
                return result

        except Exception as e:
            logger.error(f"Tabula extraction failed: {e}")

        return pd.DataFrame()

    def extract_with_openrouter(self, raw_text_chunk: str) -> str:
        """
        Fallback: use llm for messy text shit into json
        :param raw_text_chunk:
        :return:
        """

        if not self.llm_client:
            return ""

        system_prompt = """
        You are a Data Extraction Agent.
        Task: Extract financial transaction details from the provided US Congress Disclosure text.
        Output: A raw JSON list of objects. No markdown formatting.
        Schema: [{"ticker": "STR", "asset_name": "STR", "transaction_type": "Purchase/Sale", "amount_range": "$1k-$15k"}]
        If ticker is missing, use '---'. Ignore headers/footers.
        """

        try:
            response = self.llm_client.chat.completions.create(
                model=OPENROUTER_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Text:\n{raw_text_chunk}"},
                ],
                temprature = 0.0
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenRouter API Error: {e}")
            return ""

