"""从项目根目录执行: python -m backend [--llm ollama|vllm|auto] ..."""

from __future__ import annotations

import argparse
import os
import sys


def _strip_openai_env() -> None:
    for key in (
        "USER_RAG_OPENAI_BASE_URL",
        "OPENAI_BASE_URL",
        "USER_RAG_VLLM_HOST",
        "USER_RAG_VLLM_PORT",
    ):
        os.environ.pop(key, None)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="启动 User RAG SQL Agent API（uvicorn）。用 --llm 选择本机 Ollama 或远程 vLLM/OpenAI 兼容服务。",
    )
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=8000, help="端口，默认 8000")
    parser.add_argument("--reload", action="store_true", help="开发热重载")
    parser.add_argument(
        "--llm",
        choices=("auto", "ollama", "vllm"),
        default=argparse.SUPPRESS,
        help="不传则沿用环境变量 USER_RAG_LLM_BACKEND；ollama=本机 Ollama；vllm=OpenAI 兼容；auto=按是否配置 OpenAI Base URL 自动选",
    )
    parser.add_argument(
        "--vllm-url",
        default="",
        help="如 http://10.147.252.6:7777/v1；可改用环境变量 USER_RAG_OPENAI_BASE_URL",
    )
    parser.add_argument("--vllm-model", default="", help="覆盖 USER_RAG_OPENAI_MODEL，如 Qwen3-30B")
    parser.add_argument("--ollama-url", default="", help="覆盖 OLLAMA_BASE_URL")
    parser.add_argument("--ollama-model", default="", help="覆盖 OLLAMA_MODEL")
    args = parser.parse_args()

    if args.ollama_url.strip():
        os.environ["OLLAMA_BASE_URL"] = args.ollama_url.strip().rstrip("/")
    if args.ollama_model.strip():
        os.environ["OLLAMA_MODEL"] = args.ollama_model.strip()

    llm_choice = getattr(args, "llm", None)
    if llm_choice == "ollama":
        os.environ["USER_RAG_LLM_BACKEND"] = "ollama"
        _strip_openai_env()
    elif llm_choice == "vllm":
        os.environ["USER_RAG_LLM_BACKEND"] = "openai"
        if args.vllm_url.strip():
            os.environ["USER_RAG_OPENAI_BASE_URL"] = args.vllm_url.strip().rstrip("/")
        if args.vllm_model.strip():
            os.environ["USER_RAG_OPENAI_MODEL"] = args.vllm_model.strip()
        from .agent import _openai_compat_base_url

        if not _openai_compat_base_url():
            parser.error(
                "--llm vllm 需要 --vllm-url，或预先设置 USER_RAG_OPENAI_BASE_URL / OPENAI_BASE_URL / USER_RAG_VLLM_HOST+USER_RAG_VLLM_PORT"
            )
    elif llm_choice == "auto":
        os.environ.pop("USER_RAG_LLM_BACKEND", None)
        if args.vllm_url.strip():
            os.environ["USER_RAG_OPENAI_BASE_URL"] = args.vllm_url.strip().rstrip("/")
        if args.vllm_model.strip():
            os.environ["USER_RAG_OPENAI_MODEL"] = args.vllm_model.strip()
    else:
        if args.vllm_url.strip():
            os.environ["USER_RAG_OPENAI_BASE_URL"] = args.vllm_url.strip().rstrip("/")
        if args.vllm_model.strip():
            os.environ["USER_RAG_OPENAI_MODEL"] = args.vllm_model.strip()

    try:
        import uvicorn
    except ImportError:
        print("未安装 uvicorn，请先: pip install -r requirements.txt", file=sys.stderr)
        raise SystemExit(1) from None

    uvicorn.run(
        "backend.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
