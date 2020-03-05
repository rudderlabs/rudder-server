/* eslint-disable import/first */
import * as React from 'react';
declare var LeaderLine: any;
import { ButtonText, Container } from './styles';
import { Header } from '@components/common/typography';
import SourceCard from '@components/sourceCard';
import theme from '@css/theme';
import { ISourcesListStore } from '@stores/sourcesList';
import { ISourceStore } from '@stores/source';
import { ReactComponent as Plus } from '@svg/plus.svg';
import { inject, observer } from 'mobx-react';
import { Link } from 'react-router-dom';
import { withTheme } from 'styled-components';

export interface ISourcesListProps {
  sourcesListStore?: ISourcesListStore;
  theme: any;
  linesMap: any;
}

@inject('sourcesListStore')
@observer
class SourcesList extends React.Component<ISourcesListProps> {
  linesMap: any;

  constructor(props: ISourcesListProps) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    this.linesMap = this.props.linesMap;
  }

  /* componentWillUnmount() {
    this.removeDestConnectionLines();
  } */

  componentDidUpdate() {
    this.linesMap = this.props.linesMap;
  }

  /* drawDestConnectionLines = () => {
    let existingCombos = Object.keys(this.linesMap);
    let combos: string[] = [];
    this.props.sourcesListStore!.sources.forEach(source => {
      source.destinations.forEach(dest => {
        if (dest.state != 'deleting') {
          combos.push(`${source.id}-${dest.id}`);
        }
      });
    });
    existingCombos.forEach(c => {
      if (!combos.includes(c)) {
        this.linesMap[c].remove();
        delete this.linesMap[c];
      }
    });
    combos.forEach(c => {
      if (!existingCombos.includes(c)) {
        let line = new LeaderLine(
          document.getElementById(`fake-source-${c.split('-')[0]}`),
          document.getElementById(`fake-destination-${c.split('-')[1]}`),
          { endPlug: 'behind', color: this.props.theme.color.grey100, size: 4 },
        );
        this.linesMap[c] = line;
      }
    });
  };

  removeDestConnectionLines = () => {
    Object.values(this.linesMap).forEach((l: any) => l.remove());
    this.linesMap = {};
  }; */

  onMouseEnter = (source: any) => {
    Object.keys(this.linesMap).forEach(key => {
      if (key.startsWith(source.id)) {
        this.linesMap[key].setOptions({
          color: this.props.theme.color.primary,
        });
      } else {
        this.linesMap[key].setOptions({
          size: 0.01,
        });
      }
    });
  };

  onMouseLeave = (source: any) => {
    Object.values(this.linesMap).forEach((line: any) => {
      line.setOptions({
        color: this.props.theme.color.grey100,
        size: 4,
      });
    });
  };

  deleteSource = (source: ISourceStore) => {
    // useful logs
    // console.log('source is to be deleted');
    // console.log(source.name);
    const { sourcesListStore } = this.props;
    sourcesListStore!.deleteSource(source);
  };

  public render() {
    const { sourcesListStore } = this.props;
    const sources = sourcesListStore && sourcesListStore.sources;
    return (
      <Container id="sources-list" style={{ zIndex: 1 }}>
        <Header color={theme.color.grey300} className="m-b-md">
          Sources
        </Header>
        {!sources || sources.length === 0 ? (
          <div className="p-t-md">
            <SourceCard source={null} key={undefined} />
          </div>
        ) : (
          <div className="p-t-md">
            {sources.map(source => (
              <SourceCard
                source={source}
                key={source.name}
                onDelete={this.deleteSource}
                onMouseEnter={this.onMouseEnter}
                onMouseLeave={this.onMouseLeave}
              />
            ))}
            <Link to="/sources/setup" className="d-block p-t-sm">
              <Plus />
              <ButtonText> ADD SOURCE</ButtonText>
            </Link>
          </div>
        )}
      </Container>
    );
  }
}

export default withTheme(SourcesList);
