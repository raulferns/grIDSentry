import os
import sys
import io
import time
import json
import base64
import traceback
import contextlib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
NB_OUT_PATH = os.path.join(PROJECT_ROOT, "BDA_MiniProject.ipynb")

class NotebookBuilder:
    def __init__(self):
        self.cells = []
        self.execution_count = 1
        self.globals_env = {
            "__name__": "__main__",
            "PROJECT_ROOT": PROJECT_ROOT
        }

    def add_markdown(self, source_text):
        lines = [line + "\n" for line in source_text.strip().split("\n")]
        if lines:
            lines[-1] = lines[-1].rstrip("\n")
        self.cells.append({
            "cell_type": "markdown",
            "metadata": {},
            "source": lines
        })

    def add_and_execute_code(self, code_text):
        first_line = code_text.strip().split("\n")[0] if code_text.strip() else "<empty>"
        print(f"\n--- [Cell {self.execution_count}] {first_line[:75]} ---")

        lines = [line + "\n" for line in code_text.strip().split("\n")]
        if lines:
            lines[-1] = lines[-1].rstrip("\n")

        stdout_capture = io.StringIO()
        outputs = []
        plt.close('all')

        start_t = time.time()
        try:
            with contextlib.redirect_stdout(stdout_capture), contextlib.redirect_stderr(stdout_capture):
                exec(code_text, self.globals_env)
        except Exception as e:
            traceback.print_exc(file=stdout_capture)
            print(f"ERROR in cell {self.execution_count}: {e}")

        elapsed = time.time() - start_t
        stdout_val = stdout_capture.getvalue()
        if stdout_val:
            out_lines = [l + "\n" for l in stdout_val.split("\n")]
            if out_lines and out_lines[-1] == "\n":
                out_lines.pop()
            outputs.append({
                "name": "stdout",
                "output_type": "stream",
                "text": out_lines
            })

        # Capture any matplotlib figures
        fig_nums = plt.get_fignums()
        for fnum in fig_nums:
            fig = plt.figure(fnum)
            buf = io.BytesIO()
            fig.savefig(buf, format="png", bbox_inches="tight", dpi=120)
            buf.seek(0)
            img_b64 = base64.b64encode(buf.read()).decode("utf-8")
            plt.close(fig)
            outputs.append({
                "data": {
                    "image/png": img_b64,
                    "text/plain": [f"<Figure size {fig.get_size_inches()[0]*100}x{fig.get_size_inches()[1]*100} with {len(fig.axes)} Axes>"]
                },
                "metadata": {},
                "output_type": "display_data"
            })

        self.cells.append({
            "cell_type": "code",
            "execution_count": self.execution_count,
            "metadata": {},
            "outputs": outputs,
            "source": lines
        })
        print(f"-> Completed in {elapsed:.2f}s | Outputs: {len(outputs)} (Figs: {len(fig_nums)})")
        self.execution_count += 1

    def save(self, filepath):
        nb_json = {
            "cells": self.cells,
            "metadata": {
                "language_info": {
                    "name": "python",
                    "version": "3.12"
                },
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(nb_json, f, indent=1)
        print(f"\n=======================================================")
        print(f" Notebook written to: {filepath}")
        print(f" Total cells: {len(self.cells)} ({sum(1 for c in self.cells if c['cell_type'] == 'code')} code cells)")
        print(f"=======================================================\n")
