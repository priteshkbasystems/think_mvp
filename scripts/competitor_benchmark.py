from scripts.transformation_performance_index import TransformationPerformanceIndex


class CompetitorBenchmark:

    def __init__(self):
        print("Loading Competitor Benchmark Engine")

    def generate(self):

        engine = TransformationPerformanceIndex()

        scores = engine.compute()

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        report = []

        report.append("Transformation Competitor Benchmark\n")

        for rank,(bank,score) in enumerate(ranked,1):

            report.append(f"{rank}. {bank} — {score}")

        best = ranked[0][0]
        worst = ranked[-1][0]

        report.append("\nTop Performer: "+best)
        report.append("Weakest Performer: "+worst)

        return "\n".join(report)