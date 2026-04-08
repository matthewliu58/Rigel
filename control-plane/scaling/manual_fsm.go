package scaling

import (
	"log/slog"
)

func (s *Scaler) ManualScaling(pre, action, ip, vmName string) {

	if !s.tryMu.TryLock() {
		s.logger.Warn("Cannot get lock", slog.String("pre", pre))
		return
	}
	defer s.tryMu.Unlock()

	vm_ := VM{}
	if ip != "" && vmName != "" {
		s.logger.Info("ManualScaling", slog.String("pre", pre),
			slog.String("ip", ip), slog.String("VM", vmName))
		vm_.VMName = vmName
		vm_.PublicIP = ip
	}

	switch action {
	case ActionScaleUp:
		ok, vm := s.triggerScalingFromInit(1, vm_, pre, s.logger)
		if vm.PublicIP == "" {
			s.logger.Error("Create vm failed", slog.String("pre", pre))
		} else if vm.PublicIP != "" && !ok {
			s.logger.Warn("Create vm success but deploy failed",
				slog.String("pre", pre), slog.String("vm", vm.PublicIP))
		} else if vm.PublicIP != "" && ok {
			s.logger.Info("TriggerScalingFromInit success", slog.String("pre", pre))
		}
	case ActionSleep:
		s.triggerDormant(vm_, pre)
	case ActionReStart:
		s.triggerScalingFromDormant(vm_, pre)
	case ActionRelease:
		s.triggerRelease(vm_, pre)
	default:
		s.logger.Warn("The action is nonexist", slog.String("pre", pre), slog.String("odd action", action))
	}

	s.scalerDump(pre, s.logger)
	s.logger.Info("ManualScaling state change", slog.String("pre", pre),
		slog.String("old state", s.ManualAction), slog.String("new state", action))
	s.ManualAction = action
}
