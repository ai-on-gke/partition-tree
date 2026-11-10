/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cache

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestUpsert_NodeHealthEpisodePerNode walks one node through a not-ready <->
// unreachable swap, unchanged re-Upserts, a re-added taint (missed recovery),
// a recovery and a delete, and checks the cached outage start after each step.
func TestUpsert_NodeHealthEpisodePerNode(t *testing.T) {
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *metav1.Time { m := metav1.NewTime(t0.Add(d)); return &m }
	entry := func(taints ...corev1.Taint) NodeEntry {
		e := mkEntry("poolA", "mig", "root", "leaf")
		e.Taints = taints
		return e
	}
	noExec := func(key string, added *metav1.Time) corev1.Taint {
		return corev1.Taint{Key: key, Effect: corev1.TaintEffectNoExecute, TimeAdded: added}
	}
	startOf := func(c *Cache, node, key string) *metav1.Time {
		e, ok := c.Get(node)
		if !ok {
			return nil
		}
		for i := range e.Taints {
			if e.Taints[i].Key == key && e.Taints[i].Effect == corev1.TaintEffectNoExecute {
				return e.Taints[i].TimeAdded
			}
		}
		return nil
	}

	c := New()
	steps := []struct {
		name  string
		node  string
		apply func()
		key   string
		want  *metav1.Time
	}{
		{"not-ready at T0", "n1", func() { c.Upsert("n1", entry(noExec(corev1.TaintNodeNotReady, at(0)))) },
			corev1.TaintNodeNotReady, at(0)},
		{"swap to unreachable at T0+50s keeps T0", "n1", func() { c.Upsert("n1", entry(noExec(corev1.TaintNodeUnreachable, at(50*time.Second)))) },
			corev1.TaintNodeUnreachable, at(0)},
		{"unchanged unreachable re-Upsert keeps T0", "n1", func() { c.Upsert("n1", entry(noExec(corev1.TaintNodeUnreachable, at(50*time.Second)))) },
			corev1.TaintNodeUnreachable, at(0)},
		{"swap back to not-ready at T0+90s keeps T0", "n1", func() { c.Upsert("n1", entry(noExec(corev1.TaintNodeNotReady, at(90*time.Second)))) },
			corev1.TaintNodeNotReady, at(0)},
		{"not-ready re-added at T0+200s (missed recovery) starts a new episode", "n1", func() { c.Upsert("n1", entry(noExec(corev1.TaintNodeNotReady, at(200*time.Second)))) },
			corev1.TaintNodeNotReady, at(200 * time.Second)},
		{"other node's swap history does not leak onto n2", "n2", func() { c.Upsert("n2", entry(noExec(corev1.TaintNodeUnreachable, at(210*time.Second)))) },
			corev1.TaintNodeUnreachable, at(210 * time.Second)},
		{"recovery then not-ready at T0+300s is a new episode", "n1", func() {
			c.Upsert("n1", entry())
			c.Upsert("n1", entry(noExec(corev1.TaintNodeNotReady, at(300*time.Second))))
		}, corev1.TaintNodeNotReady, at(300 * time.Second)},
		{"delete then unreachable at T0+400s is a new episode", "n1", func() {
			c.Delete("n1")
			c.Upsert("n1", entry(noExec(corev1.TaintNodeUnreachable, at(400*time.Second))))
		}, corev1.TaintNodeUnreachable, at(400 * time.Second)},
	}
	for _, s := range steps {
		s.apply()
		if got := startOf(c, s.node, s.key); got == nil || !got.Equal(s.want) {
			t.Fatalf("%s: %s %s TimeAdded = %v, want %v", s.name, s.node, s.key, got, s.want)
		}
	}
}

// TestUpsert_NodeHealthEpisodeLeavesOtherTaintsAndInputAlone checks that only
// node-health NoExecute taints are carried and that the caller's slice is not
// modified when a carry applies.
func TestUpsert_NodeHealthEpisodeLeavesOtherTaintsAndInputAlone(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	c := New()
	first := mkEntry("poolA", "mig", "root", "leaf")
	first.Taints = []corev1.Taint{
		{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute, TimeAdded: &t0},
		{Key: "example.com/maint", Effect: corev1.TaintEffectNoExecute, TimeAdded: &t0},
	}
	c.Upsert("n1", first)

	second := mkEntry("poolA", "mig", "root", "leaf")
	second.Taints = []corev1.Taint{
		{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoExecute, TimeAdded: &t1},
		{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule, TimeAdded: &t1},
		{Key: "example.com/other", Effect: corev1.TaintEffectNoExecute, TimeAdded: &t1},
	}
	c.Upsert("n1", second)

	if !second.Taints[0].TimeAdded.Equal(&t1) {
		t.Errorf("caller's taint TimeAdded mutated to %v, want %v", second.Taints[0].TimeAdded, t1)
	}
	got, _ := c.Get("n1")
	want := []*metav1.Time{&t0, &t1, &t1}
	for i := range want {
		if got.Taints[i].TimeAdded == nil || !got.Taints[i].TimeAdded.Equal(want[i]) {
			t.Errorf("cached %s:%s TimeAdded = %v, want %v", got.Taints[i].Key, got.Taints[i].Effect, got.Taints[i].TimeAdded, want[i])
		}
	}
}
