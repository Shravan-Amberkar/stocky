// This program tries to reach the OMS without the RMS. It must not compile;
// TestBypassingTheRMSDoesNotCompile asserts exactly that.
package main

import "github.com/Shravan-Amberkar/stocky/internal/rms/internal/oms"

func main() { _ = oms.New }
