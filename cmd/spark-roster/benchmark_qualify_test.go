package main

import "testing"

func TestQualificationRequiresNearLimitUsageAndEveryMarker(t *testing.T) {
	markers := [3]string{"early-seal", "middle-seal", "late-seal"}
	answer := "early-seal\nmiddle-seal\nlate-seal"
	prompt, completion := 244000, 25
	good := benchSpeed{PromptTokens: &prompt, CompletionTokens: &completion, FinishReason: "stop"}
	if err := qualVerdict(good, answer, markers, 262144-qualOutput); err != nil {
		t.Fatal(err)
	}
	if err := qualVerdict(good, "early-seal\nlate-seal", markers, 262144-qualOutput); err == nil {
		t.Fatal("missing middle marker passed")
	}
	for _, n := range []int{100000, 262144} {
		prompt = n
		if err := qualVerdict(good, answer, markers, 262144-qualOutput); err == nil {
			t.Fatalf("out-of-band rendered length %d passed", n)
		}
	}
	prompt = 245000
	good.FinishReason = "length"
	if err := qualVerdict(good, answer, markers, 262144-qualOutput); err == nil {
		t.Fatal("truncated answer passed")
	}
}
