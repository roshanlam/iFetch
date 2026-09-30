# Record docs/assets/ifetch-demo.gif — fixture-backed first-run path.
# Rebuild: PATH="$HOME/go/bin:$PATH" xvfb-run -a vhs docs/demo/demo.tape

Output docs/assets/ifetch-demo.gif

Set Shell bash
Set FontSize 14
Set Width 860
Set Height 540
Set Padding 10
Set Theme "Catppuccin Mocha"
Set PlaybackSpeed 1.15
Set TypingSpeed 8ms

Hide
Type "cd /workspace/iFetch && clear"
Enter
Sleep 150ms
Show

Type "ifetch Documents ~/icloud-backup"
Sleep 900ms

Hide
Type "clear"
Enter
Type ".venv/bin/python docs/demo/fixture_run.py"
Enter
Sleep 200ms
Show

Sleep 12s
Sleep 1.8s
