from typing import List
import google.generativeai as genai
from docling.datamodel.pipeline_options import PipelineOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer
from docling_core.types.doc import (
    DoclingDocument
)
from docling.chunking import HybridChunker
from fastembed import SparseTextEmbedding, LateInteractionTextEmbedding
from transformers import AutoTokenizer
from docling.datamodel.accelerator_options import AcceleratorOptions, AcceleratorDevice
from settings import (
    LOCAL_STORAGE_PATH,
    GOOGLE_API_KEY
)
import fitz
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

accelerator_options = AcceleratorOptions(device=AcceleratorDevice.CUDA, num_threads=8)

# Mengatur PipelineOptions dan meneruskan opsi akselerator
pipeline_options = PdfPipelineOptions()
pipeline_options.accelerator_options = accelerator_options
pipeline_options.do_ocr = False
pipeline_options.do_table_structure = False
pipeline_options.table_structure_options.do_cell_matching = False
pipeline_options.document_timeout

GEMINI_EMBEDDING_MODEL = "models/text-embedding-004"
MODEL_ID = "nomic-ai/nomic-embed-text-v2-moe"
MODEL_NAME = "nomic-embed-text-v2-moe"
LOCAL_TOKENIZER_PATH = f"{LOCAL_STORAGE_PATH}/tokenizer/{MODEL_NAME}"

genai.configure(api_key=GOOGLE_API_KEY)

class DocumentProcessor:
    def __init__(self):
        self._converter = DocumentConverter(format_options={
            InputFormat.PDF: PdfFormatOption(
                accelerator_options=accelerator_options
            )
        })

        # save tokenizer to local
        if not os.path.exists(LOCAL_TOKENIZER_PATH):
            tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
            tokenizer.save_pretrained(LOCAL_TOKENIZER_PATH)

            self._tokenizer = HuggingFaceTokenizer(
                tokenizer=AutoTokenizer.from_pretrained(pretrained_model_name_or_path=LOCAL_TOKENIZER_PATH),
                max_tokens=512
            )
        else:
            self._tokenizer = HuggingFaceTokenizer(
                tokenizer=AutoTokenizer.from_pretrained(pretrained_model_name_or_path=LOCAL_TOKENIZER_PATH),
                max_tokens=512
            )
        self._chunker = HybridChunker(
            tokenizer=self._tokenizer,
        )

        self._initialize_models()

    def _initialize_models(self):
        try:
            self._gemini_embbeding_model = genai
            self._bm25_embbeding_model = SparseTextEmbedding(model_name="Qdrant/bm25",cache_dir=f"./{LOCAL_STORAGE_PATH}/models/bm25")
            self._late_interaction_embedding_model = LateInteractionTextEmbedding(model_name="colbert-ir/colbertv2.0", cache_dir=f"./{LOCAL_STORAGE_PATH}/models/colbert")
            logging.info(f"All models initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize Gemini model: {e}")
            raise e

    def _split_pdf_to_temp_files(self, file_path: str, filename: str, page_size: int = 10):
        doc = fitz.open(file_path)
        temp_files = []
        page_offsets = []  # simpan offset per part
        total_pages = doc.page_count

        if not os.path.exists(f"{LOCAL_STORAGE_PATH}/temp"):
            os.makedirs(f"{LOCAL_STORAGE_PATH}/temp")

        for i in range(0, total_pages, page_size):
            end_page = min(i + page_size, total_pages)

            new_pdf = fitz.open()
            new_pdf.insert_pdf(doc, from_page=i, to_page=end_page - 1)

            temp_file_path = f"{LOCAL_STORAGE_PATH}/temp/part_{i // page_size + 1}_{filename}"
            new_pdf.save(temp_file_path)
            new_pdf.close()
            temp_files.append(temp_file_path)

            page_offsets.append(i)  # simpan halaman awal dari part ini

        doc.close()
        return temp_files, page_offsets


    def process(self, file_path: str, filename: str):
        temp_files, page_offsets  = self._split_pdf_to_temp_files(file_path, filename)
        total_pages = fitz.open(file_path).page_count
        full_doc = None
        for idx, temp_file in enumerate(temp_files):
            try:
                logging.info(f"Processing temporary file: {temp_file}")
                doc = self._converter.convert(source=temp_file).document
                
                offset = page_offsets[idx]
                updated_pages = {}
                for page in doc.pages.values():
                    page.page_no += offset # Update nomor halaman
                    updated_pages[page.page_no] = page # Simpan dengan key (nomor halaman) yang baru
                
                # Ganti dictionary pages di objek doc saat ini dengan yang sudah dikoreksi
                doc.pages = updated_pages

                # 2. Logika penggabungan yang baru
                if full_doc is None:
                    # Jika ini adalah dokumen pertama yang berhasil diproses,
                    # jadikan ia sebagai dokumen utama.
                    full_doc = doc
                else:
                    # Untuk dokumen-dokumen berikutnya, gabungkan kamus 'pages' mereka
                    # ke dalam dokumen utama.
                    full_doc.pages.update(doc.pages)

            except Exception as e:
                logging.error(f"Failed to convert temporary file {temp_file}: {e}")
                continue
            finally:
                try:
                    os.remove(temp_file)
                except OSError:
                    pass

       
        logging.info(f"Final merged pages: {list(full_doc.pages.keys())}")
        # logging.info(f"Full doc: {full_doc.dict()}")
        chunks = list(self._chunker.chunk(dl_doc=full_doc))
        
        docs = [self._chunker.contextualize(chunk=chunk) for chunk in chunks]

        bm25_embeddings = list(self._bm25_embbeding_model.embed(docs))
        late_interaction_embeddings = list(self._late_interaction_embedding_model.embed(docs))
        gemini_embeddings_resp = self._gemini_embbeding_model.embed_content(
            content=docs,
            model=GEMINI_EMBEDDING_MODEL,
            task_type="retrieval_document"
        )
      
        gemini_embeddings = gemini_embeddings_resp['embedding']
        # logging.info(f"Processed {len(docs)} documents with {len(chunks)} chunks.")
        # logging.info(f"Docs: {docs}")
        # logging.info(f"\n\nChunks: {chunks}\n\n")
        return {
            "docs": docs,
            "chunks": chunks,
            "filename": filename,
            "bm25_vectors": bm25_embeddings,
            "colbert_vectors": late_interaction_embeddings,
            "gemini_vectors": gemini_embeddings
        }
    
    def save(self, filename):
        with open(filename, 'w') as file:
            file.write(self.process())