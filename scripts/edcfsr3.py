import sys
import time

def edcfsr(infile, outfile):
    with open(infile, "r") as fin, open(outfile, "w") as fout:
        current_obs = "none"
        for line in fin:
            line = line.rstrip("\n").ljust(255)
            cc = line[13:15]

            a = line[1:4]
            b = line[5:7]
            c = line[13:15]
            d = line[16:19]
            e = line[20:23]
            f = line[29:34]

            if a == "o-g":
                if current_obs == "none":
                    current_obs = cc
                    print("the current obs ", current_obs)
                elif current_obs != cc and "all" not in line:
                    current_obs = cc
                    print("the current obs changed to ", current_obs)
                    time.sleep(2)

            # Fix conventional header lines
            if line[0:3].strip() == "" and "it" in line and "obs" in line and "bias" in line and "pbot" not in line:
                line = " o-g it     obs use typ styp     count      bias        rms       cpen      qcpen"

            # Fix ptop
            if "ptop" in line:
                print("found the ptop")
                line = " o-g" + line
                line = line.replace("    ptop", "ptop", 1)

            # Fix pbot
            if "pbot" in line:
                line = " o-g " + line.lstrip()
                type_idx = line.find("type")
                if type_idx != -1:
                    line = line[:type_idx] + "typ" + line[type_idx+4:]
                obs_idx = line.find("obs")
                if obs_idx != -1:
                    before = line[:obs_idx+3]
                    after = line[obs_idx+3:].lstrip()
                    line = before + " use " + after

            # Fill in missing asm tags
            if a == "o-g":
                skip = d in ("asm", "rej", "mon", "all")
                if not skip:
                    if c.strip() in ("ps", "uv", "t", "q") or e.strip() == "all":
                        line = line[:16] + "asm" + line[19:]
                
                # skip surface type 'st'
                if c == "st" or cc == "st":
                    fout.write(line.strip() + "\n")
                    continue

            # Write satellite section lines without modification
            if line[1:27] == "rad total failed nonlinqc=":
                fout.write(line.strip() + "\n")
                for line in fin:
                    fout.write(line.rstrip("\n") + "\n")
                    if line[12:32] == "satellite instrument":
                        break
                continue

            # Write the processed line
            fout.write(line.strip() + "\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python edcfsr.py <input_file> <output_file>")
        sys.exit(1)

    infile = sys.argv[1]
    outfile = sys.argv[2]
    edcfsr(infile, outfile)