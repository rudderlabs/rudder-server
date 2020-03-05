import { Flex } from '@components/common/misc';
import styled from 'styled-components';

export const Container = styled(Flex).attrs({
  flexDirection: 'column',
})`
  margin: 50px 0px 50px 0px;
`;

export const HeadingTag = styled.div`
  font-size: ${({ theme }) => theme.fontSize.normal};
`;

export const MainHeading = styled.div`
  font-size: ${({ theme }) => theme.fontSize.h1};
  color: ${({ theme }) => theme.color.primary};
`;

export const TableViewContainer = styled.div`
  margin: 35px 0px 0px 0px;
  background-color: ${({ theme }) => theme.color.white};
  border-radius: 10px;
  td:nth-child(1) {
    margin-left: 35px;
    width: 30px;
  }
  .ant-table-row {
    img {
      max-height: 32px;
      max-width: 32px;
    }
    &:hover {
      cursor: pointer;
    }
  }
`;

export const IconStyle = styled.span`
  width: 60px;
`;

export const AddSource = styled.div`
  background: transparent !important;
  color: ${props => props.theme.color.primary} !important;
  font-weight: ${({ theme }) => theme.fontWeight.bold};
`;

export const IconSpacing = styled.span`
  padding: 0px 5px 0px 5px;
`;