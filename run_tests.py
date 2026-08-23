"""
run_tests.py
Ejecutor autónomo de pruebas para CivicMesh (compatible con pytest y unittest/ejecución directa).
Permite verificar el 100% de la suite sin requerir herramientas externas preinstaladas.
"""

import asyncio
import inspect
import os
import sys
import time
import traceback
from pathlib import Path

# Configurar encoding seguro para terminales Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))


def run_all_tests():
    test_files = [
        "tests/unit/test_gossip.py",
        "tests/unit/test_pubsub.py",
        "tests/unit/test_domain_a.py",
        "tests/unit/test_domain_b.py",
        "tests/integration/test_mesh_convergence.py",
        "tests/integration/test_peer_failure.py",
    ]

    total_tests = 0
    passed_tests = 0
    failed_tests = 0

    print("=" * 70)
    print("           CIVICMESH - EJECUTOR DE PRUEBAS DE VERIFICACION           ")
    print("=" * 70)
    print(f"Directorio de trabajo: {ROOT.resolve()}")
    print(f"Fecha y hora: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    for test_file_rel in test_files:
        test_path = ROOT / test_file_rel
        if not test_path.exists():
            print(f"[-] No se encontro {test_file_rel}")
            continue

        mod_name = test_file_rel.replace("/", ".").replace("\\", ".").replace(".py", "")
        print(f"\n[*] Ejecutando: {test_file_rel}")
        print("-" * 70)

        try:
            # Importar módulo dinámicamente
            if mod_name in sys.modules:
                del sys.modules[mod_name]
            mod = __import__(mod_name, fromlist=["*"])
        except Exception as e:
            print(f"[!] Error al importar {mod_name}: {e}")
            traceback.print_exc()
            failed_tests += 1
            continue

        # Encontrar funciones de test
        test_funcs = [
            (name, func) for name, func in inspect.getmembers(mod, inspect.isfunction)
            if name.startswith("test_")
        ]

        for name, func in test_funcs:
            total_tests += 1
            t0 = time.time()
            try:
                if inspect.iscoroutinefunction(func):
                    asyncio.run(func())
                else:
                    func()
                elapsed = (time.time() - t0) * 1000
                print(f"  [PASSED] {name:<45} ({elapsed:.1f} ms)")
                passed_tests += 1
            except Exception as e:
                elapsed = (time.time() - t0) * 1000
                print(f"  [FAILED] {name:<45} ({elapsed:.1f} ms)")
                print(f"     Detalle del fallo: {e}")
                traceback.print_exc()
                failed_tests += 1

    print("\n" + "=" * 70)
    print(f"RESUMEN FINAL: Total: {total_tests} | Pasaron: {passed_tests} | Fallaron: {failed_tests}")
    print("=" * 70)

    return 0 if failed_tests == 0 else 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
