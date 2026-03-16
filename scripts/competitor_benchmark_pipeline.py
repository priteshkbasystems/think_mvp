from scripts.competitor_benchmark import CompetitorBenchmark


def main():

    engine = CompetitorBenchmark()

    report = engine.generate()

    print("\nCompetitor Benchmark Report\n")

    print(report)

    with open("competitor_benchmark_report.txt","w") as f:
        f.write(report)