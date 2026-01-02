import logging
import os
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures._base import Future
from functools import partial
from pathlib import Path

from pdf2image import convert_from_path
from PIL import Image
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)


def convert_single_pdf(
    pdf_path: Path,
    output_root: Path,
    temp_root: Path,
    dpi: int = 300,
    fmt: str = "webp",
) -> str:
    """
    Converts a single PDF. Returns the pdf name for logging.
    If the PDF has only one page, it keeps the original filename.
    """
    base_name = pdf_path.stem

    # Create unique temp dir for THIS process
    pdf_temp_dir = temp_root / f"{base_name}_{os.getpid()}"
    pdf_temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        # NOTE: thread_count is set to 1 here because we are parallelizing
        # at the FILE level, not the PAGE level.
        page_paths = convert_from_path(
            str(pdf_path),
            dpi=dpi,
            output_folder=pdf_temp_dir,
            thread_count=1,
            fmt="ppm",
            paths_only=True,
        )

        num_pages: int = len(page_paths)

        for i, page_temp_path in enumerate(page_paths):
            # Check if it's a single page to determine naming convention
            if num_pages == 1:
                final_name: str = f"{base_name}.{fmt}"
            else:
                final_name: str = f"{base_name}_page_{i + 1:02d}.{fmt}"

            final_path: Path = output_root / final_name

            if final_path.exists():
                continue

            with Image.open(page_temp_path) as img:
                if fmt == "webp":
                    # Lossless high-quality settings for webp
                    img.save(final_path, format=fmt, lossless=True, quality=100)
                else:
                    img.save(final_path, format=fmt)

        return f"SUCCESS: {base_name}"

    except Exception as e:
        logger.error(f"Failed to convert {pdf_path.name}: {e}")
        return f"ERROR: {base_name}"

    finally:
        if pdf_temp_dir.exists():
            shutil.rmtree(pdf_temp_dir)


def process_batch_parallel(
    input_dir: Path, output_dir: Path, dpi: int = 300, output_format: str = "webp"
) -> None:
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    temp_dir: Path = output_dir.parent / ".temp_conversion_buffer"
    output_dir.mkdir(parents=True, exist_ok=True)

    pdf_files: list[Path] = list(input_dir.glob("*.pdf"))
    if not pdf_files:
        logger.warning(f"No PDF files found in {input_dir}")
        return

    # Optimization for Ryzen 9 5900X (12 Cores / 24 Threads)
    max_workers = 20

    logger.info(f"Processing {len(pdf_files)} PDFs with {max_workers} processes...")

    worker_func: partial[str] = partial(
        convert_single_pdf,
        output_root=output_dir,
        temp_root=temp_dir,
        dpi=dpi,
        fmt=output_format,
    )

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures: list[Future[str]] = [
            executor.submit(worker_func, pdf) for pdf in pdf_files
        ]
        for future in tqdm(
            as_completed(futures), total=len(pdf_files), desc="Converting PDFs"
        ):
            # Results are processed as they complete
            pass

    # Cleanup main temp buffer
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    logger.info(f"Batch processing complete. Output: {output_dir}")


if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent.parent
    INPUT_DIR = BASE_DIR / "assets" / "scans" / "pdf"
    OUTPUT_DIR = BASE_DIR / "assets" / "scans" / "webp"

    print(f"Looking for PDFs in: {INPUT_DIR}")
    print(f"Outputting WebP to: {OUTPUT_DIR}")

    # Run the parallel version
    process_batch_parallel(INPUT_DIR, OUTPUT_DIR, dpi=300, output_format="webp")
